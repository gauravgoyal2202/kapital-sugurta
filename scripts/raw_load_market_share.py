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
EXCEL_PATH = os.path.join(
    BASE_DIR,
    "excel_drop",
    "market_share",
    "Страх отчет(I чорак 2025 йил ) uz.xlsx",
)
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
    total_premium_2024,
    total_premium_2025,
    total_premium_change_pct,
    claims_paid_2024,
    claims_paid_2025,
    claims_paid_change_pct,
    insurance_liabilities_2024,
    insurance_liabilities_2025,
    liabilities_change_pct
) VALUES (
    %(source_file_name)s,
    %(source_sheet_name)s,
    %(excel_row_number)s,
    %(insurance_type_name)s,
    %(total_premium_2024)s,
    %(total_premium_2025)s,
    %(total_premium_change_pct)s,
    %(claims_paid_2024)s,
    %(claims_paid_2025)s,
    %(claims_paid_change_pct)s,
    %(insurance_liabilities_2024)s,
    %(insurance_liabilities_2025)s,
    %(liabilities_change_pct)s
)
ON CONFLICT (source_file_name, excel_row_number) DO NOTHING
"""

DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    id                          SERIAL PRIMARY KEY,
    source_file_name            VARCHAR(500),
    source_sheet_name           VARCHAR(100),
    excel_row_number            INTEGER,
    insurance_type_name         VARCHAR(500),
    total_premium_2024          NUMERIC(20,3),
    total_premium_2025          NUMERIC(20,3),
    total_premium_change_pct    VARCHAR(20),
    claims_paid_2024            NUMERIC(20,3),
    claims_paid_2025            NUMERIC(20,3),
    claims_paid_change_pct      VARCHAR(20),
    insurance_liabilities_2024  NUMERIC(20,3),
    insurance_liabilities_2025  NUMERIC(20,3),
    liabilities_change_pct      VARCHAR(20),
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_file_name, excel_row_number)
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

        record = {
            "source_file_name": filename,
            "source_sheet_name": SHEET_NAME,
            "excel_row_number": int(idx),
            # Col 0 – insurance type name (keep as string)
            "insurance_type_name":  clean_pct(row.iloc[0]) if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip() not in ("", "nan") else None,
            # Numeric columns
            "total_premium_2024":         clean_numeric(row.iloc[1]),
            "total_premium_2025":         clean_numeric(row.iloc[2]),
            # Percent strings
            "total_premium_change_pct":   clean_pct(row.iloc[3]),
            # Numeric columns
            "claims_paid_2024":           clean_numeric(row.iloc[4]),
            "claims_paid_2025":           clean_numeric(row.iloc[5]),
            # Percent strings
            "claims_paid_change_pct":     clean_pct(row.iloc[6]),
            # Numeric columns
            "insurance_liabilities_2024": clean_numeric(row.iloc[7]),
            "insurance_liabilities_2025": clean_numeric(row.iloc[8]),
            # Percent strings
            "liabilities_change_pct":     clean_pct(row.iloc[9]),
        }
        records.append(record)

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

    # Validate source file exists
    if not os.path.exists(EXCEL_PATH):
        log.error(f"Excel file not found: {EXCEL_PATH}")
        raise FileNotFoundError(EXCEL_PATH)

    filename = os.path.basename(EXCEL_PATH)
    log.info(f"Processing file: {filename}")

    # DB setup
    conn = get_db_connection()
    create_table(conn)

    # Extract
    df = read_excel(EXCEL_PATH)
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

    summary = (
        f"ETL complete. {inserted} rows loaded into "
        f"{SCHEMA}.{TABLE}"
    )
    log.info(summary)
    print(summary)


if __name__ == "__main__":
    main()
