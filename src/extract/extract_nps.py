import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Set up paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'load_nps.log')

# Configure logging
logging.basicConfig(
    filename=LOG_FILE, 
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# ── Metadata DB audit ─────────────────────────────────────────────────────────
sys.path.insert(0, BASE_DIR)
from src.utils.etl_utils import init_metadata_table, start_metadata_log, end_metadata_log

TARGET_TABLE = 'nps_survey_responses'

def main():
    logging.info('--- Started loading NPS data ---')

    # Initialise audit table (idempotent — safe to call every run)
    try:
        init_metadata_table()
    except Exception as e:
        logging.warning(f"Could not init metadata table: {e}")

    # Start audit log — status = RUNNING
    run_id = None
    try:
        run_id, _ = start_metadata_log(
            target_table=TARGET_TABLE,
            refresh_type='FULL',
            source_table='gdrive_nps_excel',
            load_strategy='TRUNCATE_AND_RELOAD'
        )
    except Exception as e:
        logging.warning(f"Could not start metadata log: {e}")

    # Load .env variables
    env_path = os.path.join(BASE_DIR, '.env')
    load_dotenv(env_path)

    pg_host = os.getenv('PG_HOST')
    pg_port = os.getenv('PG_PORT', '5432')
    pg_db = os.getenv('PG_DATABASE')
    pg_user = os.getenv('PG_USER')
    pg_pass = os.getenv('PG_PASSWORD')

    if not all([pg_host, pg_db, pg_user, pg_pass]):
        msg = "Missing Database configuration in .env file."
        logging.error(msg)
        if run_id:
            end_metadata_log(run_id, 'FAILED', error_message=msg)
        sys.exit(1)

    import urllib.parse
    pg_pass_encoded = urllib.parse.quote_plus(pg_pass)
    engine_url = f"postgresql://{pg_user}:{pg_pass_encoded}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(engine_url)

    # Define source file path from Google Drive
    gdrive_link = os.getenv('GDRIVE_NPS_LINK')
    if not gdrive_link:
        msg = "GDRIVE_NPS_LINK not found in .env"
        logging.error(msg)
        if run_id:
            end_metadata_log(run_id, 'FAILED', error_message=msg)
        sys.exit(1)

    import gdown
    import glob
    import shutil

    gdrive_download_dir = os.path.join(BASE_DIR, 'data', 'raw', 'gdrive_nps')
    # Clear directory to ensure we only process the latest download
    if os.path.exists(gdrive_download_dir):
        shutil.rmtree(gdrive_download_dir)
    os.makedirs(gdrive_download_dir, exist_ok=True)

    logging.info(f"Downloading Google Drive folder from {gdrive_link}...")
    try:
        cwd = os.getcwd()
        os.chdir(gdrive_download_dir)
        gdown.download_folder(gdrive_link, quiet=True, use_cookies=False)
        os.chdir(cwd)
    except Exception as e:
        msg = f"Failed to download from Google Drive: {e}"
        logging.error(msg, exc_info=True)
        if run_id:
            end_metadata_log(run_id, 'FAILED', error_message=msg)
        sys.exit(1)

    search_pattern = os.path.join(gdrive_download_dir, '**', '*.xlsx')
    excel_files = glob.glob(search_pattern, recursive=True)

    if not excel_files:
        msg = "No Excel file found for NPS in downloaded Drive folder."
        logging.error(msg)
        if run_id:
            end_metadata_log(run_id, 'FAILED', error_message=msg)
        sys.exit(1)

    excel_path = excel_files[0]
    logging.info(f"Found Excel file: {excel_path}")

    try:
        # Read Sheets
        sheets = {
            'Form Responses 1': 'General NPS',
            'Form Responses 2': 'General NPS',
            'Form Responses 3': 'Employee NPS'
        }

        dfs = []
        for sheet, survey_type in sheets.items():
            try:
                df = pd.read_excel(excel_path, sheet_name=sheet)
                if len(df) > 0:
                    df = df.iloc[:, [0, 1, 2]]
                    df.columns = ['response_timestamp', 'nps_score_raw', 'comment_text']
                    df['survey_type'] = survey_type
                    df['source_sheet'] = sheet
                    dfs.append(df)
            except Exception as e:
                logging.warning(f"Could not read sheet {sheet}: {e}")

        if not dfs:
            logging.info("No data found in any sheet.")
            if os.path.exists(gdrive_download_dir):
                shutil.rmtree(gdrive_download_dir)
            if run_id:
                end_metadata_log(run_id, 'SUCCESS', rows_extracted=0, rows_inserted=0)
            return

        combined = pd.concat(dfs, ignore_index=True)

        combined['response_timestamp'] = pd.to_datetime(combined['response_timestamp'], errors='coerce')
        combined['nps_score_raw'] = pd.to_numeric(combined['nps_score_raw'], errors='coerce')
        combined['comment_text'] = combined['comment_text'].astype(str)

        # Add metadata
        combined['etl_loaded_at'] = pd.Timestamp.now()

        schema_name = 'raw'
        table_name = 'nps_survey_responses'

        from sqlalchemy import text

        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    id               SERIAL PRIMARY KEY,
                    response_timestamp TIMESTAMP,
                    nps_score_raw    NUMERIC(4,1),
                    comment_text     TEXT,
                    survey_type      VARCHAR(50),
                    source_sheet     VARCHAR(50),
                    etl_loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))

        logging.info(f"Writing data to PostgreSQL table {schema_name}.{table_name} (transactional)...")
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(
                    f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY;"
                ))
                logging.info(f"Table {schema_name}.{table_name} truncated (PK sequence reset).")
                combined.to_sql(
                    table_name,
                    con=conn,
                    schema=schema_name,
                    if_exists='append',
                    index=False
                )
                logging.info(f"Inserted {len(combined)} rows — committing transaction.")

        logging.info("Data dumped to database successfully.")

        if os.path.exists(gdrive_download_dir):
            shutil.rmtree(gdrive_download_dir)
            logging.info(f"Cleaned up temporary download directory: {gdrive_download_dir}")

        # ── Audit: SUCCESS ────────────────────────────────────────────────────
        if run_id:
            end_metadata_log(run_id, 'SUCCESS', rows_extracted=len(combined), rows_inserted=len(combined))

    except Exception as e:
        msg = f"Failed to load NPS data: {e}"
        logging.error(msg, exc_info=True)
        if run_id:
            end_metadata_log(run_id, 'FAILED', error_message=msg)
        sys.exit(1)

    logging.info('--- Loading completed ---')

if __name__ == '__main__':
    main()
