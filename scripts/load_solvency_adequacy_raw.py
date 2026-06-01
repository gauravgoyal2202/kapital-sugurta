import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Set up paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'load_solvency_adequacy.log')

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

def main():
    logging.info('--- Started loading Solvency Adequacy data ---')
    
    # Load .env variables
    env_path = os.path.join(BASE_DIR, '.env')
    load_dotenv(env_path)
    
    pg_host = os.getenv('PG_HOST')
    pg_port = os.getenv('PG_PORT', '5432')
    pg_db = os.getenv('PG_DATABASE')
    pg_user = os.getenv('PG_USER')
    pg_pass = os.getenv('PG_PASSWORD')
    
    if not all([pg_host, pg_db, pg_user, pg_pass]):
        logging.error("Missing Database configuration in .env file.")
        sys.exit(1)
        
    import urllib.parse
    pg_pass_encoded = urllib.parse.quote_plus(pg_pass)
    engine_url = f"postgresql://{pg_user}:{pg_pass_encoded}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(engine_url)
    
    # Define source file path from Google Drive
    gdrive_link = os.getenv('GDRIVE_SOLVENCY_ADEQUACY_LINK')
    if not gdrive_link:
        logging.error("GDRIVE_SOLVENCY_ADEQUACY_LINK not found in .env")
        sys.exit(1)

    import gdown
    import glob
    import shutil
    
    gdrive_download_dir = os.path.join(BASE_DIR, 'excel_drop', 'gdrive_solvency')
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
        logging.error(f"Failed to download from Google Drive: {e}", exc_info=True)
        sys.exit(1)

    search_pattern = os.path.join(gdrive_download_dir, '**', '*.xlsx')
    excel_files = glob.glob(search_pattern, recursive=True)
    
    if not excel_files:
        logging.error(f"No Excel file found for Solvency Adequacy in downloaded Drive folder.")
        sys.exit(1)
        
    excel_path = excel_files[0]
    logging.info(f"Found Excel file: {excel_path}")
        
    try:
        # Read Excel: row 1 is header (header=0 in pandas), row 2 is data
        logging.info(f"Reading data from {excel_path}...")
        df = pd.read_excel(excel_path, header=0)
        
        # Log basic stats
        logging.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from Excel.")
        logging.info(f"Columns found: {', '.join(df.columns.tolist())}")
        
        # Add metadata
        df['loaded_at'] = pd.Timestamp.now()
        
        # Load into Postgres (raw schema)
        schema_name = 'raw'
        table_name = 'solvency_adequacy'
        
        logging.info(f"Writing data to PostgreSQL table {schema_name}.{table_name}...")
        df.to_sql(
            table_name, 
            con=engine, 
            schema=schema_name, 
            if_exists='replace', 
            index=False
        )
        logging.info("Data dumped to database successfully.")
        
        if os.path.exists(gdrive_download_dir):
            shutil.rmtree(gdrive_download_dir)
            logging.info(f"Cleaned up temporary download directory: {gdrive_download_dir}")
        
        
    except Exception as e:
        logging.error(f"Failed to load Solvency Adequacy data: {e}", exc_info=True)
        sys.exit(1)
        
    logging.info('--- Loading completed ---')

if __name__ == '__main__':
    main()
