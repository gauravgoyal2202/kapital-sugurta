"""
raw_load_market_share.py
------------------------
ETL: reads sheet "1.4" from the market share Excel file and loads every data
row (after the 4 header rows) into raw.market_share_insurance_class_stats.

Usage:
    python scripts/raw_load_market_share.py

Expects a .env file in the project root with:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import os
import math
import logging
import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths & logging
# ---------------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "raw_load_market_share.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SHEET_NAME = "1.4"

# Rows (0-based pandas index) that are header / title — skip them for insert
HEADER_ROW_INDICES = {0, 1, 2, 3}

SCHEMA = "raw"
TABLE = "market_share_insurance_class_stats"

INSERT_SQL = f"""
INSERT INTO {SCHEMA}.{TABLE} (
    source_file_name,
    source_sheet_name,
    excel_row_number,
    insurance_type_name,
    report_year,
    total_premium,
    total_premium_change_pct,
    claims_paid,
    claims_paid_change_pct,
    insurance_liabilities,
    liabilities_change_pct
) VALUES (
    %(source_file_name)s,
    %(source_sheet_name)s,
    %(excel_row_number)s,
    %(insurance_type_name)s,
    %(report_year)s,
    %(total_premium)s,
    %(total_premium_change_pct)s,
    %(claims_paid)s,
    %(claims_paid_change_pct)s,
    %(insurance_liabilities)s,
    %(liabilities_change_pct)s
)
ON CONFLICT (source_file_name, excel_row_number, report_year) DO NOTHING
"""

DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    id                          SERIAL PRIMARY KEY,
    source_file_name            VARCHAR(500),
    source_sheet_name           VARCHAR(100),
    excel_row_number            INTEGER,
    insurance_type_name         VARCHAR(500),
    report_year                 INTEGER,
    total_premium               NUMERIC(20,3),
    total_premium_change_pct    VARCHAR(20),
    claims_paid                 NUMERIC(20,3),
    claims_paid_change_pct      VARCHAR(20),
    insurance_liabilities       NUMERIC(20,3),
    liabilities_change_pct      VARCHAR(20),
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_file_name, excel_row_number, report_year)
);
"""


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def clean_numeric(val):
    """
    Convert a raw cell string to float.
    Returns None for NaN, None, empty, '-', ' - ', or any parse error.
    """
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    s = str(val).strip().replace(",", "")
    if s in ("", "-", " - ", "nan", "None"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def clean_pct(val):
    """
    Return the percent string as-is (e.g. '+18.8%', '-22.2%', '0%').
    Returns None for NaN / empty values.
    Strips surrounding whitespace but keeps sign and % symbol.
    """
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    s = str(val).strip()
    if s in ("", "nan", "None"):
        return None
    return s


# ---------------------------------------------------------------------------
# Core ETL functions
# ---------------------------------------------------------------------------

def get_db_connection():
    """Load .env from project root and return a psycopg2 connection."""
    env_path = os.path.join(BASE_DIR, ".env")
    load_dotenv(env_path)

    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    dbname = os.getenv("PG_DATABASE")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    missing = [k for k, v in {
        "DB_HOST": host, "DB_NAME": dbname,
        "DB_USER": user, "DB_PASSWORD": password,
    }.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required .env variables: {', '.join(missing)}"
        )

    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    conn.autocommit = False
    log.info("Database connection established.")
    return conn


def create_table(conn):
    """Run the CREATE TABLE IF NOT EXISTS DDL."""
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    log.info(f"Table {SCHEMA}.{TABLE} is ready.")


def read_excel(filepath):
    """
    Read the Excel sheet with header=None so every row comes through as-is.
    All cells are read as raw strings (dtype=str).
    Returns the full DataFrame (all rows).
    """
    log.info(f"Reading Excel file: {filepath}")
    df = pd.read_excel(
        filepath,
        sheet_name=SHEET_NAME,
        header=None,
        skiprows=0,
        dtype=str,
    )
    log.info(f"Total rows read from Excel (including header rows): {len(df)}")
    return df


def transform_rows(df, filename):
    """
    Convert the DataFrame rows into a list of dicts ready for INSERT.
    Skips the 4 header/title rows; processes all remaining rows.
    """
    records = []
    skipped = 0

    for idx, row in df.iterrows():
        if idx in HEADER_ROW_INDICES:
            skipped += 1
            continue

        insurance_type = clean_pct(row.iloc[0]) if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip() not in ("", "nan") else None

        # 2024 Record
        record_2024 = {
            "source_file_name": filename,
            "source_sheet_name": SHEET_NAME,
            "excel_row_number": int(idx),
            "insurance_type_name": insurance_type,
            "report_year": 2024,
            "total_premium": clean_numeric(row.iloc[1]),
            "total_premium_change_pct": None,
            "claims_paid": clean_numeric(row.iloc[4]),
            "claims_paid_change_pct": None,
            "insurance_liabilities": clean_numeric(row.iloc[7]),
            "liabilities_change_pct": None,
        }
        records.append(record_2024)

        # 2025 Record
        record_2025 = {
            "source_file_name": filename,
            "source_sheet_name": SHEET_NAME,
            "excel_row_number": int(idx),
            "insurance_type_name": insurance_type,
            "report_year": 2025,
            "total_premium": clean_numeric(row.iloc[2]),
            "total_premium_change_pct": clean_pct(row.iloc[3]),
            "claims_paid": clean_numeric(row.iloc[5]),
            "claims_paid_change_pct": clean_pct(row.iloc[6]),
            "insurance_liabilities": clean_numeric(row.iloc[8]),
            "liabilities_change_pct": clean_pct(row.iloc[9]),
        }
        records.append(record_2025)

    log.info(f"Header rows skipped: {skipped}")
    log.info(f"Data rows to insert: {len(records)}")
    return records, skipped


def insert_rows(conn, rows):
    """
    Batch-insert rows using executemany.
    Tracks successes and per-row failures.
    Returns (inserted_count, failed_rows).
    """
    inserted = 0
    failed = []

    with conn.cursor() as cur:
        for record in rows:
            try:
                cur.execute(INSERT_SQL, record)
                inserted += 1
            except Exception as exc:
                conn.rollback()
                log.warning(
                    f"  Row excel_row_number={record['excel_row_number']} "
                    f"failed: {exc}"
                )
                failed.append({"excel_row_number": record["excel_row_number"], "error": str(exc)})

        conn.commit()

    return inserted, failed


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("ETL START: raw_load_market_share")
    log.info("=" * 60)

    # DB setup (loads .env)
    conn = get_db_connection()
    create_table(conn)

    gdrive_link = os.getenv('GDRIVE_MARKET_SHARE_LINK')
    if not gdrive_link:
        log.error("GDRIVE_MARKET_SHARE_LINK not found in .env")
        raise EnvironmentError("GDRIVE_MARKET_SHARE_LINK missing")

    import gdown
    import glob
    import shutil
    
    gdrive_download_dir = os.path.join(BASE_DIR, 'excel_drop', 'gdrive_market_share')
    # Clear directory to ensure we only process the latest download
    if os.path.exists(gdrive_download_dir):
        shutil.rmtree(gdrive_download_dir)
    os.makedirs(gdrive_download_dir, exist_ok=True)
    
    log.info(f"Downloading Google Drive folder from {gdrive_link}...")
    try:
        cwd = os.getcwd()
        os.chdir(gdrive_download_dir)
        gdown.download_folder(gdrive_link, quiet=True, use_cookies=False)
        os.chdir(cwd)
    except Exception as e:
        log.error(f"Failed to download from Google Drive: {e}", exc_info=True)
        raise

    search_pattern = os.path.join(gdrive_download_dir, '**', '*.xlsx')
    excel_files = glob.glob(search_pattern, recursive=True)
    
    if not excel_files:
        log.error(f"No Excel file found for market_share in downloaded Drive folder.")
        raise FileNotFoundError("market_share Excel file missing")
        
    excel_path = excel_files[0]
    log.info(f"Found Excel file: {excel_path}")

    filename = os.path.basename(excel_path)
    log.info(f"Processing file: {filename}")

    # Extract
    df = read_excel(excel_path)
    total_rows = len(df)
    log.info(f"Total rows read from Excel: {total_rows}")

    # Transform
    rows, skipped_count = transform_rows(df, filename)
    log.info(f"Rows skipped (header/title): {skipped_count}")

    # Load
    inserted, failed = insert_rows(conn, rows)

    # Summary
    if failed:
        log.warning(f"{len(failed)} row(s) failed to insert:")
        for f in failed:
            log.warning(f"  excel_row_number={f['excel_row_number']}: {f['error']}")

    conn.close()

    if os.path.exists(gdrive_download_dir):
        shutil.rmtree(gdrive_download_dir)
        log.info(f"Cleaned up temporary download directory: {gdrive_download_dir}")

    summary = (
        f"ETL complete. {inserted} rows loaded into "
        f"{SCHEMA}.{TABLE}"
    )
    log.info(summary)
    print(summary)


if __name__ == "__main__":
    main()
