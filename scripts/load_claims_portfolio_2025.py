"""
load_claims_portfolio_2025.py
─────────────────────────────
Loads the sheet "Страховые события Общее" (sheet index 0) from the 2025
portfolio Excel file into raw.claims_portfolio — the same table used by the
existing ingestion pipeline.

Why a separate script?
  • The 2025 file has a 4-row merged header block (rows 1-4); data starts row 5.
  • Columns A-P (cols 1-16) are KPI summary totals, NOT per-claim data.
  • Per-claim fields begin at column Q (0-based index 16).
  • Only the fields present in the existing FIXED_2021_COLUMNS schema are
    extracted; all other columns are silently dropped.

Usage
  # Recreate table and load (default):
  python load_claims_portfolio_2025.py /path/to/file.xlsx

  # Append to existing table:
  python load_claims_portfolio_2025.py /path/to/file.xlsx --append

  # Parse only, no DB write:
  python load_claims_portfolio_2025.py /path/to/file.xlsx --dry-run
"""

import os
import sys
import math
import logging
import argparse
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ── env & logging ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

os.makedirs(os.path.join(BASE_DIR, 'logs'), exist_ok=True)
LOG = os.path.join(BASE_DIR, 'logs', 'watch_excel.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG, encoding='utf-8'),
        logging.StreamHandler(),
    ],
)

PG_CONN = (
    f"host={os.getenv('PG_HOST')} port={os.getenv('PG_PORT', '5432')} "
    f"dbname={os.getenv('PG_DATABASE') or os.getenv('PG_DB', 'postgres')} "
    f"user={os.getenv('PG_USER')} "
    f"password='{os.getenv('PG_PASSWORD')}'"
)

# ── target schema (must match FIXED_2021_COLUMNS in the existing script) ───────
FIXED_2021_COLUMNS = [
    'record_id', 'insured_region', 'insured_branch', 'reporting_region',
    'claim_reported_by', 'claim_report_date', 'grouping_code', 'insurance_type',
    'insurance_class', 'entity_type', 'taxpayer_id', 'client_name',
    'injured_party', 'contract_id', 'policy_id', 'incident_date',
    'reported_damage_property', 'reported_damage_health', 'damage_adjustment_notes',
    'incident_cause', 'settlement_order_id', 'settlement_decision_date',
    'payout_date', 'payout_property', 'payout_health', 'payout_total',
    'currency', 'rejection_date', 'rejection_reason', 'claim_id',
    'claim_expense_amount', 'outstanding_loss_amount', 'damaged_object',
    'recovery_amount', 'claim_handler', 'notes', 'beneficiary',
    'vehicle_engine_type', 'branch_name', 'claim_submission_date',
]

# ── column mapping ─────────────────────────────────────────────────────────────
#
# Excel layout (sheet "Страховые события Общее"):
#   Rows 1-4  = merged header block (skipped via skiprows=4)
#   Row  5+   = per-claim data rows (loaded)
#
#   Col A  (0-based  0) = Row sequence / ID
#   Cols B-P (0-based 1-15) = Aggregate KPI counters — NOT per-claim, SKIPPED
#   Col Q  (0-based 16) = Филиал / Branch
#   Col V  (0-based 21) = Вид страхования / Insurance type
#   Col X  (0-based 23) = Product (КАСКО, ОСАГО …)
#   Col Y  (0-based 24) = КЛАСС Страхования / Insurance class
#   Col Z  (0-based 25) = № Договор / Contract ID
#   Col AA (0-based 26) = Страхователь / Policyholder (client_name)
#   Col AB (0-based 27) = Заявитель / Claimant (injured_party)
#   Col AC (0-based 28) = ПИНФЛ / ИНН заявителя / Taxpayer ID
#   Col AD (0-based 29) = Объект / Insured item (damaged_object)
#   Col AE (0-based 30) = Дата и время события / Incident date-time
#   Col AJ (0-based 35) = Тип страхователя / Entity type (юр./физ.)
#   Col AM (0-based 38) = Номер регистрации / Claim registration number
#   Col AN (0-based 39) = Дата принятия / Claim acceptance date
#   Col AO (0-based 40) = Дата выплаты / Payment date
#   Col AP (0-based 41) = Сумма убытков / Loss / (previous now change) payout total
#   Col AQ (0-based 42) = payout total
#   Col AS (0-based 44) = Пользователь / Handler / user
#   Col AZ (0-based 51) = Сумма взыскания / Recovery amount
#   Col BF (0-based 57) = Бенефициар / Beneficiary
#   Col BM (0-based 64) = Статус / Status → stored in notes
#
# Format: list of (0-based_col_index, schema_field_name)
# When multiple source columns map to the same field, the LAST entry wins.
# ──────────────────────────────────────────────────────────────────────────────
_COL_FIELD_PAIRS = [
    (0,  'record_id'),
    (16, 'branch_name'),
    (21, 'insurance_type'),
    (22, 'grouping_code'),
    (23, 'policy_id'),
    (24, 'insurance_class'),
    (25, 'contract_id'),
    (26, 'client_name'),
    (27, 'injured_party'),
    (28, 'taxpayer_id'),
    (29, 'damaged_object'),
    (30, 'incident_date'),
    (35, 'entity_type'),
    (38, 'claim_id'),
    (39, 'claim_submission_date'),
    (40, 'payout_date'),
    (42, 'payout_total'),
    (44, 'claim_handler'),
    (51, 'recovery_amount'),
    (57, 'beneficiary'),
    (64, 'notes'),
]

# Build bidirectional maps
_field_to_col: dict = {}
for _ci, _f in _COL_FIELD_PAIRS:
    _field_to_col[_f] = _ci             # last-write wins per field

COL_TO_FIELD: dict = {v: k for k, v in _field_to_col.items()}  # col_idx -> field

SHEET_NAME = 'Страховые события Общее'
SKIP_ROWS  = 4   # rows 1-4 are the merged header block; data starts row 5


# ── helpers ────────────────────────────────────────────────────────────────────
def _safe_str(val):
    """Convert any value to a clean string, or None if empty / NaN."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if hasattr(val, 'isoformat'):
        s = val.isoformat()
        return None if s == 'NaT' else s
    s = str(val).strip()
    return s or None


# ── loader ─────────────────────────────────────────────────────────────────────
def load_portfolio_2025(path: str):
    """
    Read SHEET_NAME from path and return a DataFrame aligned to FIXED_2021_COLUMNS.
    Columns with no source mapping are filled with None.
    Returns None on failure.
    """
    logging.info(f"Reading '{SHEET_NAME}' from {os.path.basename(path)}")

    try:
        df_raw = pd.read_excel(
            path,
            sheet_name=SHEET_NAME,
            header=None,
            skiprows=SKIP_ROWS,
            engine='openpyxl',
        )
    except Exception as exc:
        logging.error(f"Failed to open Excel: {exc}")
        return None

    logging.info(f"Raw shape after skipping {SKIP_ROWS} header rows: {df_raw.shape}")

    extractable_fields = set(COL_TO_FIELD.values())
    unmapped_fields    = [f for f in FIXED_2021_COLUMNS if f not in extractable_fields]

    records = []
    for _, row in df_raw.iterrows():
        rec = {}

        for col_idx, field in COL_TO_FIELD.items():
            raw_val = row.iloc[col_idx] if col_idx < len(row) else None
            rec[field] = _safe_str(raw_val)

        for field in unmapped_fields:
            rec[field] = None

        records.append(rec)

    df = pd.DataFrame(records, columns=FIXED_2021_COLUMNS)

    # Drop rows where every extractable field is None (pure empty rows)
    df.dropna(how='all', subset=list(extractable_fields), inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Attach pipeline meta columns
    df['source_file'] = f"{os.path.basename(path)} | {SHEET_NAME}"
    df['loaded_at']   = datetime.now().isoformat()

    logging.info(f"Extracted {len(df)} rows ready for ingestion")
    logging.info("Non-null counts per extracted field:")
    for field in sorted(extractable_fields):
        nn = df[field].notna().sum()
        logging.info(f"  {field:<35} {nn:>5} / {len(df)}")

    return df


# ── postgres writer (mirrors df_to_pg in the existing script) ─────────────────
def df_to_pg(df, table: str, pg, schema: str = 'raw', append: bool = False):
    if df is None or df.empty:
        logging.warning("Empty DataFrame — nothing to insert.")
        return

    cur = pg.cursor()
    try:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        cols     = list(df.columns)
        col_defs = ', '.join([f'"{c}" TEXT' for c in cols])

        cur.execute(
            "SELECT EXISTS(SELECT FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s)",
            (schema, table),
        )
        table_exists = cur.fetchone()[0]

        if not append or not table_exists:
            if not append:
                cur.execute(f'DROP TABLE IF EXISTS {schema}."{table}"')
            cur.execute(f'CREATE TABLE {schema}."{table}" ({col_defs})')
        else:
            # Add any columns missing from the existing table
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            existing_cols = {r[0] for r in cur.fetchall()}
            for c in cols:
                if c not in existing_cols:
                    cur.execute(f'ALTER TABLE {schema}."{table}" ADD COLUMN "{c}" TEXT')
                    logging.info(f"  Added missing column: {c}")

        rows = [
            tuple(
                str(v) if (v is not None and not (isinstance(v, float) and math.isnan(v)))
                else None
                for v in record
            )
            for record in df.itertuples(index=False)
        ]

        if rows:
            cols_sql = ', '.join([f'"{c}"' for c in cols])
            execute_values(
                cur,
                f'INSERT INTO {schema}."{table}" ({cols_sql}) VALUES %s',
                rows,
            )

        pg.commit()
        logging.info(f"✓  {schema}.{table}: {len(df)} rows inserted")

    except Exception as exc:
        pg.rollback()
        logging.error(f"DB error: {exc}")
        raise
    finally:
        cur.close()


# ── entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Load 2025 claims portfolio Excel into raw.claims_portfolio"
    )
    parser.add_argument('file', help='Path to the .xlsx file')
    parser.add_argument(
        '--append', action='store_true',
        help='Append to existing table (default: drop + recreate)',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Parse and log stats without writing to the database',
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        logging.error(f"File not found: {args.file}")
        sys.exit(1)

    logging.info('─── 2025 portfolio ingestion started ───')

    df = load_portfolio_2025(args.file)
    if df is None or df.empty:
        logging.error("No data extracted — aborting.")
        sys.exit(1)

    if args.dry_run:
        logging.info("Dry-run mode — skipping DB write.")
        print(df.head(5).to_string())
        return

    pg = psycopg2.connect(PG_CONN)
    try:
        df_to_pg(df, 'claims_portfolio', pg, schema='raw', append=args.append)
    finally:
        pg.close()

    logging.info('─── 2025 portfolio ingestion done ───')


if __name__ == '__main__':
    main()
