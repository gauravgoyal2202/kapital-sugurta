"""
Load Kapital Sugurta FM (financial model) plan data from Excel into PostgreSQL.

Phase 1:
  - Time spine from sheet 'Время и флаги' (monthly plan periods Jul 2025 – Dec 2028)
  - Consolidated insurance / P&L metrics from sheet 'ФО' (company totals block)

Phase 2:
  - Direct premium by FM product group from sheet 'Страхование' (6 product groups)

Phase 3:
  - Solvency block from sheet 'Регулятор'
  - Investment portfolio / income from sheet 'Инвестиция'
  - Operating and financial income rows from sheet 'ФО'

Phase 4:
  - Commercial initiatives from sheet 'Коммерческие инициативы'
  - Reinsurance initiatives from sheet 'Инициативы по перестрахованию'
  - Back-office initiatives from sheet 'Инициативы бек-офиса'

Target tables (raw schema):
  - fm_plan_time_spine
  - fm_plan_metrics
  - fm_plan_initiatives
"""

from __future__ import annotations

import glob
import logging
import os
import shutil
import sys
import urllib.parse
from datetime import datetime
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "load_fm_plan.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.getLogger("").addHandler(console)

sys.path.insert(0, BASE_DIR)
from src.extract.fm_plan_config import (
    BACKOFFICE_INITIATIVE_LAYOUT,
    BACKOFFICE_INITIATIVES_SECTION,
    BACKOFFICE_INITIATIVES_SHEET,
    COEFFICIENTS_METRIC_ROWS,
    COEFFICIENTS_SECTION,
    COEFFICIENTS_SHEET,
    COMMERCIAL_INITIATIVE_LAYOUT,
    COMMERCIAL_INITIATIVES_SECTION,
    COMMERCIAL_INITIATIVES_SHEET,
    FM_PRODUCT_GROUPS,
    FO_EXTENDED_METRIC_ROWS,
    FO_RESERVE_METRIC_ROWS,
    FO_RESERVE_SECTION,
    INSURANCE_PRODUCT_ROWS,
    INSURANCE_SECTION,
    INSURANCE_SHEET,
    INVESTMENT_METRIC_ROWS,
    INVESTMENT_SECTION,
    INVESTMENT_SHEET,
    REGULATOR_METRIC_ROWS,
    REGULATOR_SECTION,
    REGULATOR_SHEET,
    REINSURANCE_INITIATIVE_LAYOUT,
    REINSURANCE_INITIATIVES_SECTION,
    REINSURANCE_INITIATIVES_SHEET,
)
from src.utils.etl_utils import end_metadata_log, init_metadata_table, start_metadata_log

TARGET_TABLE = "fm_plan_metrics"
INITIATIVES_TABLE = "fm_plan_initiatives"
SCHEMA = "raw"

# Monthly plan time axis on FM sheets (0-indexed openpyxl columns).
# Excel mapping (1-based): V=22 … BK=63 → 0-index 21…62.
#   P  (16) blank
#   Q:U (17–21 / 0-idx 16–20) annual / historical periods — excluded
#   V   (22 / 0-idx 21) first monthly plan period (Jul 2025)
#   AB:AM (28–39 / 0-idx 27–38) 2026 monthly plan
#   BK  (63 / 0-idx 62) last monthly plan column (Dec 2028)
DATA_COL_START = 21  # Excel V
DATA_COL_END = 62    # Excel BK

# Consolidated company totals in sheet 'ФО' (1-based Excel rows)
FO_SHEET = "ФО"
FO_SECTION = "company_consolidated"
FO_METRIC_ROWS: dict[int, str] = {
    65: "gross_direct_premium",
    66: "ceded_reinsurance_premium",
    67: "assumed_reinsurance_premium",
    68: "net_premium",
    69: "retention_rate",
    72: "earned_premium",
    87: "claims_and_commission",
    100: "insurance_activity_result",
    120: "net_profit",
}

FO_LABEL_COL = 6


def _row_label(df: pd.DataFrame, row_idx: int, label_col: int = FO_LABEL_COL) -> str:
    """Read metric label; FM sheets use cols 0–6 depending on the row."""
    if label_col < len(df.columns):
        value = df.iloc[row_idx, label_col]
        if pd.notna(value) and str(value).strip():
            return str(value).strip()
    for col in range(0, min(7, len(df.columns))):
        value = df.iloc[row_idx, col]
        if pd.notna(value) and isinstance(value, str) and len(str(value).strip()) > 2:
            return str(value).strip()
    return ""


def resolve_excel_path() -> str:
    """Return local path or download from Google Drive when configured."""
    local_path = os.getenv(
        "FM_PLAN_EXCEL_PATH",
        os.path.join(BASE_DIR, "docs", "excels", "FM_Kapital Sugurta_20251118.xlsm"),
    )
    if os.path.isfile(local_path):
        logging.info("Using local FM plan file: %s", local_path)
        return local_path

    gdrive_link = os.getenv("GDRIVE_FM_PLAN_LINK")
    if not gdrive_link:
        raise FileNotFoundError(
            f"FM plan Excel not found at {local_path} and GDRIVE_FM_PLAN_LINK is not set."
        )

    import gdown

    download_dir = os.path.join(BASE_DIR, "data", "raw", "gdrive_fm_plan")
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)

    logging.info("Downloading FM plan folder from Google Drive...")
    cwd = os.getcwd()
    try:
        os.chdir(download_dir)
        gdown.download_folder(gdrive_link, quiet=True, use_cookies=False)
    finally:
        os.chdir(cwd)

    matches = glob.glob(os.path.join(download_dir, "**", "*.xls*"), recursive=True)
    if not matches:
        raise FileNotFoundError("No .xls/.xlsm file found in downloaded FM plan folder.")

    logging.info("Downloaded FM plan file: %s", matches[0])
    return matches[0]


def _as_date(value: Any) -> datetime | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def extract_time_spine(excel_path: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name="Время и флаги", header=None, engine="openpyxl")
    rows: list[dict[str, Any]] = []

    for col in range(DATA_COL_START, min(DATA_COL_END + 1, len(df.columns))):
        period_start = _as_date(df.iloc[2, col])
        if period_start is None:
            continue

        period_end = _as_date(df.iloc[3, col])
        report_year = df.iloc[4, col]
        period_code = df.iloc[6, col]
        period_duration = df.iloc[7, col]

        try:
            report_year_int = int(report_year)
        except (TypeError, ValueError):
            report_year_int = period_start.year

        try:
            period_code_int = int(period_code)
        except (TypeError, ValueError):
            period_code_int = None

        duration = float(period_duration) if pd.notna(period_duration) else None
        is_monthly_plan = (
            period_code_int == 1
            and duration is not None
            and duration < 0.15
            and period_start >= datetime(2025, 7, 1)
        )

        rows.append(
            {
                "column_index": col,
                "period_start_date": period_start.date(),
                "period_end_date": period_end.date() if period_end else None,
                "report_year": report_year_int,
                "period_code": period_code_int,
                "period_duration": duration,
                "is_monthly_plan": is_monthly_plan,
            }
        )

    spine = pd.DataFrame(rows)
    logging.info(
        "Extracted %s time periods (%s monthly plan periods).",
        len(spine),
        int(spine["is_monthly_plan"].sum()) if not spine.empty else 0,
    )
    return spine


def extract_sheet_metrics(
    excel_path: str,
    spine: pd.DataFrame,
    *,
    sheet_name: str,
    section_name: str,
    metric_rows: dict[int, str],
    label_col: int = FO_LABEL_COL,
    product_rows: dict[int, str] | None = None,
) -> pd.DataFrame:
    """Extract long-format metrics for fixed Excel rows and monthly plan columns."""
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    monthly_cols = spine.loc[spine["is_monthly_plan"], "column_index"].tolist()
    records: list[dict[str, Any]] = []

    for excel_row, metric_code in metric_rows.items():
        row_idx = excel_row - 1
        if row_idx >= len(df):
            logging.warning("%s row %s missing — skipped.", sheet_name, excel_row)
            continue

        label_str = _row_label(df, row_idx, label_col) or metric_code

        product_group_code = None
        product_group_label = None
        if product_rows and excel_row in product_rows:
            product_group_code = product_rows[excel_row]
            product_group_label = FM_PRODUCT_GROUPS[product_group_code]["label"]

        for col in monthly_cols:
            if col >= len(df.columns):
                continue
            raw_val = df.iloc[row_idx, col]
            if pd.isna(raw_val) or not isinstance(raw_val, (int, float)):
                continue

            period_start = spine.loc[spine["column_index"] == col, "period_start_date"].iloc[0]
            records.append(
                {
                    "sheet_name": sheet_name,
                    "section_name": section_name,
                    "metric_code": metric_code,
                    "metric_label": label_str,
                    "product_group_code": product_group_code,
                    "product_group_label": product_group_label,
                    "source_row": excel_row,
                    "period_start_date": period_start,
                    "value_mln_uzs": float(raw_val),
                }
            )

    metrics = pd.DataFrame(records)
    logging.info(
        "Extracted %s FM plan metric values from %s (%s).",
        len(metrics),
        sheet_name,
        section_name,
    )
    return metrics


def extract_fo_metrics(excel_path: str, spine: pd.DataFrame) -> pd.DataFrame:
    consolidated = extract_sheet_metrics(
        excel_path,
        spine,
        sheet_name=FO_SHEET,
        section_name=FO_SECTION,
        metric_rows=FO_METRIC_ROWS,
    )
    extended = extract_sheet_metrics(
        excel_path,
        spine,
        sheet_name=FO_SHEET,
        section_name=FO_SECTION,
        metric_rows=FO_EXTENDED_METRIC_ROWS,
    )
    return pd.concat([consolidated, extended], ignore_index=True)


def extract_regulator_metrics(excel_path: str, spine: pd.DataFrame) -> pd.DataFrame:
    return extract_sheet_metrics(
        excel_path,
        spine,
        sheet_name=REGULATOR_SHEET,
        section_name=REGULATOR_SECTION,
        metric_rows=REGULATOR_METRIC_ROWS,
    )


def extract_reserve_metrics(excel_path: str, spine: pd.DataFrame) -> pd.DataFrame:
    return extract_sheet_metrics(
        excel_path,
        spine,
        sheet_name=FO_SHEET,
        section_name=FO_RESERVE_SECTION,
        metric_rows=FO_RESERVE_METRIC_ROWS,
    )


def extract_investment_metrics(excel_path: str, spine: pd.DataFrame) -> pd.DataFrame:
    return extract_sheet_metrics(
        excel_path,
        spine,
        sheet_name=INVESTMENT_SHEET,
        section_name=INVESTMENT_SECTION,
        metric_rows=INVESTMENT_METRIC_ROWS,
    )


def extract_coefficient_metrics(excel_path: str, spine: pd.DataFrame) -> pd.DataFrame:
    """Official plan ratios from sheet Коэффиценты (rows 82–86)."""
    return extract_sheet_metrics(
        excel_path,
        spine,
        sheet_name=COEFFICIENTS_SHEET,
        section_name=COEFFICIENTS_SECTION,
        metric_rows=COEFFICIENTS_METRIC_ROWS,
    )


def extract_insurance_product_metrics(excel_path: str, spine: pd.DataFrame) -> pd.DataFrame:
    product_metric_rows = {row: "gross_direct_premium" for row in INSURANCE_PRODUCT_ROWS}
    return extract_sheet_metrics(
        excel_path,
        spine,
        sheet_name=INSURANCE_SHEET,
        section_name=INSURANCE_SECTION,
        metric_rows=product_metric_rows,
        product_rows=INSURANCE_PRODUCT_ROWS,
    )


def _parse_sheet_date_columns(
    df: pd.DataFrame,
    *,
    date_row: int,
    value_start_col: int,
) -> list[tuple[int, Any]]:
    columns: list[tuple[int, Any]] = []
    for col in range(value_start_col, len(df.columns)):
        period_start = _as_date(df.iloc[date_row, col])
        if period_start is None:
            continue
        columns.append((col, period_start.date()))
    return columns


def _cell_value(df: pd.DataFrame, row_idx: int, col: int) -> float | None:
    raw_val = df.iloc[row_idx, col]
    if pd.isna(raw_val):
        return None
    if isinstance(raw_val, (int, float)):
        return float(raw_val)
    try:
        return float(str(raw_val).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None


def extract_initiative_sheet(
    excel_path: str,
    *,
    sheet_name: str,
    section_name: str,
    layout: dict[str, int],
    initiative_col: int | None = None,
    product_col: int | None = None,
    grouping_col: int | None = None,
    line_item_col: int | None = None,
) -> pd.DataFrame:
    """Extract long-format initiative rows (product × line item × month)."""
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    date_columns = _parse_sheet_date_columns(
        df,
        date_row=layout["date_row"],
        value_start_col=layout["value_start_col"],
    )
    if not date_columns:
        logging.warning("No monthly columns found on initiative sheet %s.", sheet_name)
        return pd.DataFrame()

    initiative_col = initiative_col if initiative_col is not None else layout.get("initiative_col")
    product_col = product_col if product_col is not None else layout.get("product_col")
    grouping_col = grouping_col if grouping_col is not None else layout.get("grouping_col")
    entity_type_col = layout.get("entity_type_col")
    line_item_col = line_item_col if line_item_col is not None else layout["line_item_col"]

    records: list[dict[str, Any]] = []
    current_initiative: str | None = None

    for row_idx in range(layout["data_start_row"], len(df)):
        if initiative_col is not None:
            initiative_val = df.iloc[row_idx, initiative_col]
            if pd.notna(initiative_val) and str(initiative_val).strip():
                current_initiative = str(initiative_val).strip()

        line_item_val = df.iloc[row_idx, line_item_col]
        if pd.isna(line_item_val) or not str(line_item_val).strip():
            continue
        line_item = str(line_item_val).strip()

        fm_product = None
        if product_col is not None and product_col < len(df.columns):
            product_val = df.iloc[row_idx, product_col]
            if pd.notna(product_val) and str(product_val).strip():
                fm_product = str(product_val).strip()

        grouping_name = None
        if grouping_col is not None and grouping_col < len(df.columns):
            grouping_val = df.iloc[row_idx, grouping_col]
            if pd.notna(grouping_val) and str(grouping_val).strip():
                grouping_name = str(grouping_val).strip()

        entity_type_label = None
        if entity_type_col is not None and entity_type_col < len(df.columns):
            entity_val = df.iloc[row_idx, entity_type_col]
            if pd.notna(entity_val) and str(entity_val).strip():
                entity_type_label = str(entity_val).strip()

        has_value = False
        for col, period_start in date_columns:
            value = _cell_value(df, row_idx, col)
            if value is None:
                continue
            has_value = True
            records.append(
                {
                    "sheet_name": sheet_name,
                    "section_name": section_name,
                    "initiative_name": current_initiative,
                    "fm_product": fm_product,
                    "grouping_name": grouping_name,
                    "entity_type_label": entity_type_label,
                    "line_item": line_item,
                    "source_row": row_idx + 1,
                    "period_start_date": period_start,
                    "value_mln_uzs": value,
                }
            )

        if not has_value and fm_product and line_item:
            # Keep zero-only rows out of Postgres; they add no analytic value.
            continue

    initiatives = pd.DataFrame(records)
    logging.info(
        "Extracted %s FM initiative values from %s (%s).",
        len(initiatives),
        sheet_name,
        section_name,
    )
    return initiatives


def extract_commercial_initiatives(excel_path: str) -> pd.DataFrame:
    return extract_initiative_sheet(
        excel_path,
        sheet_name=COMMERCIAL_INITIATIVES_SHEET,
        section_name=COMMERCIAL_INITIATIVES_SECTION,
        layout=COMMERCIAL_INITIATIVE_LAYOUT,
    )


def extract_reinsurance_initiatives(excel_path: str) -> pd.DataFrame:
    return extract_initiative_sheet(
        excel_path,
        sheet_name=REINSURANCE_INITIATIVES_SHEET,
        section_name=REINSURANCE_INITIATIVES_SECTION,
        layout=REINSURANCE_INITIATIVE_LAYOUT,
    )


def extract_backoffice_initiatives(excel_path: str) -> pd.DataFrame:
    return extract_initiative_sheet(
        excel_path,
        sheet_name=BACKOFFICE_INITIATIVES_SHEET,
        section_name=BACKOFFICE_INITIATIVES_SECTION,
        layout=BACKOFFICE_INITIATIVE_LAYOUT,
    )


def extract_all_initiatives(excel_path: str) -> pd.DataFrame:
    frames = [
        extract_commercial_initiatives(excel_path),
        extract_reinsurance_initiatives(excel_path),
        extract_backoffice_initiatives(excel_path),
    ]
    return pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)


def ensure_tables(engine) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA};

    CREATE TABLE IF NOT EXISTS {SCHEMA}.fm_plan_time_spine (
        column_index        INTEGER,
        period_start_date   DATE,
        period_end_date     DATE,
        report_year         INTEGER,
        period_code         INTEGER,
        period_duration     NUMERIC,
        is_monthly_plan     BOOLEAN,
        loaded_at           TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA}.fm_plan_metrics (
        sheet_name              TEXT,
        section_name            TEXT,
        metric_code             TEXT,
        metric_label            TEXT,
        product_group_code      TEXT,
        product_group_label     TEXT,
        source_row              INTEGER,
        period_start_date       DATE,
        value_mln_uzs           NUMERIC,
        loaded_at               TIMESTAMPTZ
    );

    ALTER TABLE {SCHEMA}.fm_plan_metrics
        ADD COLUMN IF NOT EXISTS product_group_code TEXT;

    ALTER TABLE {SCHEMA}.fm_plan_metrics
        ADD COLUMN IF NOT EXISTS product_group_label TEXT;

    CREATE TABLE IF NOT EXISTS {SCHEMA}.fm_plan_initiatives (
        sheet_name              TEXT,
        section_name            TEXT,
        initiative_name         TEXT,
        fm_product              TEXT,
        grouping_name           TEXT,
        entity_type_label       TEXT,
        line_item               TEXT,
        source_row              INTEGER,
        period_start_date       DATE,
        value_mln_uzs           NUMERIC,
        loaded_at               TIMESTAMPTZ
    );

    ALTER TABLE {SCHEMA}.fm_plan_initiatives
        ADD COLUMN IF NOT EXISTS entity_type_label TEXT;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def load_table(engine, table_name: str, df: pd.DataFrame) -> None:
    if df.empty:
        logging.warning("No rows to load for %s.%s", SCHEMA, table_name)
        return

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{table_name};"))
            df.to_sql(table_name, con=conn, schema=SCHEMA, if_exists="append", index=False)
    logging.info("Loaded %s rows into %s.%s", len(df), SCHEMA, table_name)


def main() -> None:
    logging.info("--- Started loading FM plan data ---")

    try:
        init_metadata_table()
    except Exception as exc:
        logging.warning("Could not init metadata table: %s", exc)

    run_id = None
    try:
        run_id, _ = start_metadata_log(
            target_table=TARGET_TABLE,
            refresh_type="FULL",
            source_table="fm_plan_excel",
            load_strategy="TRUNCATE_AND_RELOAD",
            pipeline_name="fm_plan_excel_to_postgres",
            source_system="excel",
            source_schema=None,
        )
    except Exception as exc:
        logging.warning("Could not start metadata log: %s", exc)

    load_dotenv(os.path.join(BASE_DIR, ".env"))
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DATABASE")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASSWORD")

    if not all([pg_host, pg_db, pg_user, pg_pass]):
        msg = "Missing database configuration in .env"
        logging.error(msg)
        if run_id:
            end_metadata_log(run_id, "FAILED", error_message=msg)
        sys.exit(1)

    pg_pass_encoded = urllib.parse.quote_plus(pg_pass)
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass_encoded}@{pg_host}:{pg_port}/{pg_db}"
    )

    try:
        excel_path = resolve_excel_path()
        loaded_at = pd.Timestamp.now(tz="UTC")

        spine = extract_time_spine(excel_path)
        fo_metrics = extract_fo_metrics(excel_path, spine)
        product_metrics = extract_insurance_product_metrics(excel_path, spine)
        regulator_metrics = extract_regulator_metrics(excel_path, spine)
        investment_metrics = extract_investment_metrics(excel_path, spine)
        reserve_metrics = extract_reserve_metrics(excel_path, spine)
        coefficient_metrics = extract_coefficient_metrics(excel_path, spine)
        initiatives = extract_all_initiatives(excel_path)
        metrics = pd.concat(
            [
                fo_metrics,
                product_metrics,
                regulator_metrics,
                investment_metrics,
                reserve_metrics,
                coefficient_metrics,
            ],
            ignore_index=True,
        )

        if spine.empty or metrics.empty:
            raise ValueError("FM plan extraction produced no rows — check Excel layout.")
        if initiatives.empty:
            raise ValueError("FM initiative extraction produced no rows — check Excel layout.")

        spine["loaded_at"] = loaded_at
        metrics["loaded_at"] = loaded_at
        initiatives["loaded_at"] = loaded_at

        ensure_tables(engine)
        load_table(engine, "fm_plan_time_spine", spine)
        load_table(engine, "fm_plan_metrics", metrics)
        load_table(engine, INITIATIVES_TABLE, initiatives)

        if run_id:
            end_metadata_log(
                run_id,
                "SUCCESS",
                rows_extracted=len(metrics) + len(initiatives),
                rows_inserted=len(metrics) + len(initiatives),
            )
    except Exception as exc:
        msg = f"Failed to load FM plan data: {exc}"
        logging.error(msg, exc_info=True)
        if run_id:
            end_metadata_log(run_id, "FAILED", error_message=msg)
        sys.exit(1)

    logging.info("--- FM plan loading completed ---")


if __name__ == "__main__":
    main()
