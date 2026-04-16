"""
etl_reinsurance.py
------------------
Production-ready ETL pipeline to load two Excel files into separate PostgreSQL tables.
  • File 1 (Outgoing) -> raw.reinsurance_outgoing_portfolio (112 columns)
  • File 2 (Incoming) -> raw.reinsurance_incoming_portfolio (28 columns)

Connection via env-vars in .env:
  PG_HOST, PG_PORT, PG_DATABASE (or PG_DB), PG_USER, PG_PASSWORD
"""

import os
import sys
import logging
import traceback
import math
import io
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ---------------------------------------------------------------------------
# Logging (UTF-8 safe for Windows)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8"))],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent / "excel_drop" / "Reinsurance_Level"
FILE1_NAME = "Порт 4 кв Исход 2025 уточ.xlsx" # date K amount AM (outgoing)
FILE2_NAME = "Порт 2021-2025 вход (2).xlsx" # date K amount AA (incoming)
FILE1_PATH = BASE_DIR / FILE1_NAME
FILE2_PATH = BASE_DIR / FILE2_NAME

# ---------------------------------------------------------------------------
# Column Definitions - File 1 (112 cols)
# ---------------------------------------------------------------------------
FILE1_COLUMNS = [
    "seq_number", "insurance_contract_number", "policyholder", "insurance_type",
    "voluntary_insurance_type", "mandatory_insurance_type", "region", "policyholder_country",
    "city_village", "individual_legal_entity", "contract_conclusion_date", "insured_amount_contract",
    "contract_currency", "insurance_premium_contract", "premium_currency_contract", "contract_start_date",
    "contract_end_date", "insured_amount_policy", "reinsurance_type", "insurance_premium_policy",
    "premium_currency_policy", "insurance_days_count", "reinsurance_broker", "reinsurer",
    "cedant_commission", "actual_premium_usd_paid_in_uzs", "actual_premium_eur_paid_in_uzs", "actual_premium_usd",
    "actual_premium_eur", "actual_premium_rub", "actual_premium_uzs_net_reinsurance", "premium_currency_policy2",
    "total_accrued_premium_uzs", "premium_accrual_date", "received_premium_usd", "received_premium_eur",
    "received_premium_rub", "received_premium_uzs", "total_received_premium_uzs", "premium_receipt_date",
    "policy_number_slip_number", "insurance_effective_start_date", "insurance_effective_end_date", "policy_issue_date_slip_sign_date",
    "insurance_duration_days", "notes", "bank_name_beneficiary", "reinsurer_share_pct",
    "reinsurer_or_broker_name", "reinsurer_or_broker_country", "reinsurance_start_date", "reinsurance_end_date",
    "reinsurance_duration_days", "liabilities_ceded_reinsurance", "liabilities_ceded_rub", "liabilities_ceded_eur",
    "liabilities_ceded_usd", "total_premiums_ceded_uzs", "premiums_ceded_uzs", "premiums_ceded_usd",
    "premiums_ceded_usd_star", "premiums_ceded_eur_star", "premiums_ceded_eur", "premiums_ceded_rub",
    "total_commission_received_uzs", "commission_received_uzs", "commission_received_usd", "commission_received_usd_star",
    "commission_received_eur_star", "commission_received_eur", "commission_received_rub", "nonresident_tax_usd",
    "nonresident_tax_rub", "nonresident_tax_uzs", "net_reinsurance_premium_uzs", "net_reinsurance_premium_rub",
    "net_reinsurance_premium_usd_star", "net_reinsurance_premium_eur", "net_reinsurance_premium_usd", "net_reinsurance_premium_transfer_date",
    "own_retention", "income_attribution_date", "agency_commission", "agency_commission_pct",
    "agent_name", "acquisition_accrual_date", "preventive_reserve", "commission_accrual_date",
    "mtpl_preventive_reserve_5pct", "legal_entity_status", "assistance_commission", "reinsurance_agreement_number",
    "reinsurance_agreement_date", "branch", "administrative_expenses", "base_insurance_premium",
    "insurance_contract_duration", "blank_col", "reporting_date", "contract_duration_days",
    "days_elapsed_since_effective", "days_remaining_liability", "days_remaining_liability_calc", "upr_reinsurer_share",
    "ibnr_reinsurer_share", "accounting_group", "upr_reinsurer_share_usd", "upr_reinsurer_share_eur",
    "upr_reinsurer_share_rub", "ibnr_reinsurer_share_usd", "ibnr_reinsurer_share_eur", "ibnr_reinsurer_share_rub"
]

# ---------------------------------------------------------------------------
# Column Definitions - File 2 (28 cols)
# ---------------------------------------------------------------------------
FILE2_COLUMNS = [
    "seq_number", "contract_number", "policyholder", "insurance_type",
    "voluntary_insurance_type", "mandatory_insurance_type", "region", "policyholder_country",
    "city_village", "individual_legal_entity", "contract_conclusion_date", "insured_amount_contract",
    "contract_currency", "insurance_premium_contract", "premium_currency_contract", "contract_start_date",
    "contract_end_date", "insured_amount_policy", "insured_amount_policy_usd", "insurance_premium_policy",
    "premium_currency_policy", "actual_premium_usd", "actual_premium_eur", "actual_premium_rub",
    "actual_premium_uzs", "premium_currency_policy2", "total_accrued_premium_uzs", "branch"
]

# ---------------------------------------------------------------------------
# Type mappings for DDL
# ---------------------------------------------------------------------------
DATE_COLS = {
    "contract_conclusion_date", "contract_start_date", "contract_end_date", "premium_accrual_date",
    "premium_receipt_date", "insurance_effective_start_date", "insurance_effective_end_date",
    "policy_issue_date_slip_sign_date", "reinsurance_start_date", "reinsurance_end_date",
    "net_reinsurance_premium_transfer_date", "income_attribution_date", "acquisition_accrual_date",
    "commission_accrual_date", "reinsurance_agreement_date", "reporting_date"
}

INT_COLS = {
    "insurance_days_count", "insurance_duration_days", "reinsurance_duration_days", "contract_duration_days",
    "days_elapsed_since_effective", "days_remaining_liability", "days_remaining_liability_calc"
}

NUM_COLS = {
    "insured_amount_contract", "insurance_premium_contract", "insured_amount_policy", "insurance_premium_policy",
    "cedant_commission", "actual_premium_usd_paid_in_uzs", "actual_premium_eur_paid_in_uzs", "actual_premium_usd",
    "actual_premium_eur", "actual_premium_rub", "actual_premium_uzs_net_reinsurance", "total_accrued_premium_uzs",
    "received_premium_usd", "received_premium_eur", "received_premium_rub", "received_premium_uzs",
    "total_received_premium_uzs", "reinsurer_share_pct", "liabilities_ceded_reinsurance", "liabilities_ceded_rub",
    "liabilities_ceded_eur", "liabilities_ceded_usd", "total_premiums_ceded_uzs", "premiums_ceded_uzs",
    "premiums_ceded_usd", "premiums_ceded_usd_star", "premiums_ceded_eur_star", "premiums_ceded_eur",
    "premiums_ceded_rub", "total_commission_received_uzs", "commission_received_uzs", "commission_received_usd",
    "commission_received_usd_star", "commission_received_eur_star", "commission_received_eur", "commission_received_rub",
    "nonresident_tax_usd", "nonresident_tax_rub", "nonresident_tax_uzs", "net_reinsurance_premium_uzs",
    "net_reinsurance_premium_rub", "net_reinsurance_premium_usd_star", "net_reinsurance_premium_eur",
    "net_reinsurance_premium_usd", "own_retention", "agency_commission", "agency_commission_pct",
    "preventive_reserve", "mtpl_preventive_reserve_5pct", "assistance_commission", "administrative_expenses",
    "base_insurance_premium", "upr_reinsurer_share", "ibnr_reinsurer_share", "upr_reinsurer_share_usd",
    "upr_reinsurer_share_eur", "upr_reinsurer_share_rub", "ibnr_reinsurer_share_usd", "ibnr_reinsurer_share_eur",
    "ibnr_reinsurer_share_rub", "insured_amount_policy_usd", "actual_premium_uzs"
}

def get_pg_type(col_name):
    if col_name in DATE_COLS: return "DATE"
    if col_name in INT_COLS: return "INTEGER"
    if col_name in NUM_COLS: return "NUMERIC"
    return "TEXT"

# ---------------------------------------------------------------------------
# DDL Generation
# ---------------------------------------------------------------------------
def create_ddl(table_name, columns):
    col_defs = ["id SERIAL PRIMARY KEY", "source_file TEXT"]
    for c in columns:
        col_defs.append(f'"{c}" {get_pg_type(c)}')
    return f'CREATE TABLE IF NOT EXISTS raw."{table_name}" (\n    ' + ",\n    ".join(col_defs) + "\n);"

DDL_OUTGOING = create_ddl("reinsurance_outgoing_portfolio", FILE1_COLUMNS)
DDL_INCOMING = create_ddl("reinsurance_incoming_portfolio", FILE2_COLUMNS)

# ---------------------------------------------------------------------------
# Data Cleaning Helpers
# ---------------------------------------------------------------------------
def clean_val(val, col_name):
    if pd.isna(val) or val is None:
        return None
    
    if col_name in DATE_COLS:
        try:
            dt = pd.to_datetime(val, errors='coerce')
            return dt.date() if not pd.isna(dt) else None
        except: return None
        
    if col_name in NUM_COLS:
        try:
            num = pd.to_numeric(val, errors='coerce')
            return float(num) if not pd.isna(num) else None
        except: return None
        
    if col_name in INT_COLS:
        try:
            num = pd.to_numeric(val, errors='coerce')
            return int(num) if not pd.isna(num) else None
        except: return None
        
    s = str(val).strip()
    return s if s else None

# ---------------------------------------------------------------------------
# ETL Logic
# ---------------------------------------------------------------------------
def process_file(cur, path, columns, table_name, skip_rows):
    log.info(f"Processing {os.path.basename(path)} -> raw.{table_name}")
    try:
        df = pd.read_excel(path, sheet_name=0, skiprows=skip_rows, header=None, dtype=object)
    except Exception as e:
        log.error(f"Error reading {path}: {e}")
        return 0, 0, len(columns)

    # Pad or trim columns to match expected count
    if df.shape[1] < len(columns):
        for i in range(df.shape[1], len(columns)):
            df[i] = None
    df = df.iloc[:, :len(columns)]
    df.columns = columns

    total_rows = len(df)
    insert_data = []
    skipped = 0

    for idx, row in df.iterrows():
        # Check if row is entirely empty
        if row.isna().all():
            skipped += 1
            continue
            
        cleaned_row = [os.path.basename(path)]
        for col in columns:
            cleaned_row.append(clean_val(row[col], col))
        insert_data.append(tuple(cleaned_row))

    if not insert_data:
        log.info(f"No valid data in {path}")
        return 0, skipped, total_rows

    # Build INSERT SQL
    col_str = '"source_file", ' + ", ".join([f'"{c}"' for c in columns])
    placeholder_str = ", ".join(["%s"] * (len(columns) + 1))
    insert_sql = f'INSERT INTO raw."{table_name}" ({col_str}) VALUES ({placeholder_str})'

    try:
        execute_batch(cur, insert_sql, insert_data, page_size=1000)
        cur.connection.commit()
        log.info(f"Successfully inserted {len(insert_data)} rows into raw.{table_name}")
    except Exception as e:
        cur.connection.rollback()
        log.error(f"Error inserting into {table_name}: {e}")
        return 0, skipped, total_rows

    return len(insert_data), skipped, total_rows

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    conn_params = {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", 5432)),
        "dbname": os.getenv("PG_DATABASE") or os.getenv("PG_DB", "postgres"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
    }

    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
    except Exception as e:
        log.error(f"Connection failed: {e}")
        return

    # Create Schema and Tables
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute(DDL_OUTGOING)
        cur.execute(DDL_INCOMING)
        conn.commit()
    except Exception as e:
        log.error(f"DDL failed: {e}")
        conn.rollback()
        return

    # Process File 1 (Outgoing)
    f1_res = process_file(cur, FILE1_PATH, FILE1_COLUMNS, "reinsurance_outgoing_portfolio", 7)

    # Process File 2 (Incoming)
    f2_res = process_file(cur, FILE2_PATH, FILE2_COLUMNS, "reinsurance_incoming_portfolio", 3)

    cur.close()
    conn.close()

    # Summary
    print("\n" + "="*80)
    print("ETL REINSURANCE SUMMARY")
    print("="*80)
    print(f"{'Table':<40} {'Read':>10} {'Skipped':>10} {'Inserted':>10}")
    print("-" * 80)
    print(f"{'raw.reinsurance_outgoing_portfolio':<40} {f1_res[2]:>10} {f1_res[1]:>10} {f1_res[0]:>10}")
    print(f"{'raw.reinsurance_incoming_portfolio':<40} {f2_res[2]:>10} {f2_res[1]:>10} {f2_res[0]:>10}")
    print("="*80)

if __name__ == "__main__":
    main()
