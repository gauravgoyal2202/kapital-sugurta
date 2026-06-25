import os
import sys
import json
import logging
import time
import calendar
import argparse
from datetime import datetime, date
from typing import List, Optional, Dict, Any

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
import psycopg2
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# --- Configuration & Setup ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)
load_dotenv(os.path.join(BASE_DIR, '.env'))

LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'extract_1c.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger('1C_Extractor')

class Config:
    API_HOST = os.getenv('API_1C_HOST', '10.10.1.209').replace('http://', '').replace('https://', '').rstrip('/')
    API_USER = os.getenv('API_1C_USER')
    API_PASS = os.getenv('API_1C_PASS')

    PG_HOST     = os.getenv('PG_HOST')
    PG_PORT     = os.getenv('PG_PORT', '5432')
    PG_DATABASE = os.getenv('PG_DATABASE')
    PG_USER     = os.getenv('PG_USER')
    PG_PASSWORD = os.getenv('PG_PASSWORD')

    ALERT_SMTP_HOST = os.getenv('ALERT_SMTP_HOST')
    ALERT_SMTP_PORT = os.getenv('ALERT_SMTP_PORT', '587')
    ALERT_SMTP_USER = os.getenv('ALERT_SMTP_USER')
    ALERT_SMTP_PASS = os.getenv('ALERT_SMTP_PASS')
    ALERT_FROM      = os.getenv('ALERT_FROM')
    ALERT_TO        = os.getenv('ALERT_TO')
    ALERT_CC        = os.getenv('ALERT_CC')

    @classmethod
    def validate(cls):
        missing = []
        if not cls.API_USER:    missing.append('API_1C_USER')
        if not cls.API_PASS:    missing.append('API_1C_PASS')
        if not cls.PG_DATABASE: missing.append('PG_DATABASE')
        if missing:
            logger.error(f"Missing environment variables: {', '.join(missing)}")
            sys.exit(1)

    @classmethod
    def get_pg_conn_str(cls):
        return (
            f"host={cls.PG_HOST} port={cls.PG_PORT} "
            f"dbname={cls.PG_DATABASE} user={cls.PG_USER} password={cls.PG_PASSWORD}"
        )


# --- Schema & Table DDL ---
SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.financial_performance_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.balance_sheet_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.investment_activity_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- Add updated_at column if it doesn't exist yet (safe migration)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name   = 'financial_performance_api_response'
          AND column_name  = 'updated_at'
    ) THEN
        ALTER TABLE raw.financial_performance_api_response ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name   = 'balance_sheet_api_response'
          AND column_name  = 'updated_at'
    ) THEN
        ALTER TABLE raw.balance_sheet_api_response ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name   = 'investment_activity_api_response'
          AND column_name  = 'updated_at'
    ) THEN
        ALTER TABLE raw.investment_activity_api_response ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
    END IF;
END $$;
"""

ENDPOINT_CONFIG = {
    'fp': {
        'table': 'raw.financial_performance_api_response',
        'path':  '/kapital/hs/api/Report/FinancialPerformance?Date=',
        'name':  'Financial Performance',
    },
    'bs': {
        'table': 'raw.balance_sheet_api_response',
        'path':  '/kapital/hs/api/Report/BalanceSheet?Date=',
        'name':  'Balance Sheet',
    },
    'ia': {
        'table': 'raw.investment_activity_api_response',
        'path':  '/kapital/hs/api/Report/InvestmentActivity?Date=',
        'name':  'Investment Activity',
    },
}

class OneCExtractor:
    def __init__(self):
        Config.validate()
        self.auth     = HTTPBasicAuth(Config.API_USER, Config.API_PASS)
        self.base_url = f"http://{Config.API_HOST}"
        self.session  = requests.Session()
        self.conn     = None

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------
    def connect_db(self):
        try:
            self.conn = psycopg2.connect(Config.get_pg_conn_str())
            with self.conn.cursor() as cur:
                cur.execute(SETUP_SQL)
            self.conn.commit()
            logger.info("Database connection established and schema verified.")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            sys.stderr.write(f"Database connection failed: {e}\n")
            sys.exit(1)

    def close_db(self):
        if self.conn:
            self.conn.close()

    # ------------------------------------------------------------------
    # Date helpers
    # ------------------------------------------------------------------
    def get_month_ends(self, start_date: date, end_date: date) -> List[date]:
        """Return the last day of each month between start_date and end_date."""
        curr       = start_date
        month_ends = []
        while curr <= end_date:
            last_day = calendar.monthrange(curr.year, curr.month)[1]
            dt = date(curr.year, curr.month, last_day)
            if start_date <= dt <= end_date and dt not in month_ends:
                month_ends.append(dt)
            curr = date(curr.year + 1, 1, 1) if curr.month == 12 else date(curr.year, curr.month + 1, 1)
        return month_ends

    # ------------------------------------------------------------------
    # Alerting
    # ------------------------------------------------------------------
    def send_alert_email(self, subject: str, message: str):
        if not Config.ALERT_SMTP_HOST or not Config.ALERT_TO:
            logger.warning("SMTP not configured, skipping email alert.")
            return
        try:
            msg = MIMEMultipart()
            msg['From'] = Config.ALERT_FROM or Config.ALERT_SMTP_USER or "alert@example.com"
            msg['To'] = Config.ALERT_TO
            if Config.ALERT_CC:
                msg['Cc'] = Config.ALERT_CC
            msg['Subject'] = subject
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(Config.ALERT_SMTP_HOST, int(Config.ALERT_SMTP_PORT))
            server.starttls()
            if Config.ALERT_SMTP_USER and Config.ALERT_SMTP_PASS:
                server.login(Config.ALERT_SMTP_USER, Config.ALERT_SMTP_PASS)
            
            server.send_message(msg)
            server.quit()
            logger.info("Alert email sent successfully.")
        except Exception as e:
            logger.error(f"Failed to send alert email: {e}")

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------
    def fetch_data(self, path: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{path}"
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                response = self.session.get(url, auth=self.auth, timeout=60)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                last_error = f"HTTP Error {response.status_code}: {e}"
                logger.warning(f"{last_error} for {url}")
                if response.status_code == 401:
                    logger.error("Authentication failed. Check credentials.")
                    self.send_alert_email("1C API Auth Failed", f"URL: {url}\nError: {last_error}")
                    return None
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Attempt {attempt} failed for {url}: {last_error}")
            
            if attempt < retries:
                time.sleep(2 ** attempt)
        
        self.send_alert_email("1C API Fetch Failed", f"Failed to fetch {url} after {retries} attempts.\nLast Error: {last_error}")
        return None

    # ------------------------------------------------------------------
    # DB write: UPSERT by month+year
    # ------------------------------------------------------------------
    def save_to_db(self, endpoint_code: str, data: Dict[str, Any], report_date: date) -> tuple:
        """
        Upsert logic keyed on (month, year):
          - If a row already exists for the same year+month  -> UPDATE payload, report_date (today's actual date), updated_at
          - If no row exists for that year+month             -> INSERT new row with the actual report_date

        report_date is stored as-is (the real date the script was run / the API call date),
        so it reflects the exact day the data was fetched and updates on every call.
        """
        config      = ENDPOINT_CONFIG[endpoint_code]
        table       = config['table']
        payload_str = json.dumps(data, ensure_ascii=False)

        check_sql = f"""
            SELECT id, report_date
            FROM   {table}
            WHERE  EXTRACT(YEAR  FROM report_date) = %s
              AND  EXTRACT(MONTH FROM report_date) = %s
            LIMIT 1
        """
        update_sql = f"""
            UPDATE {table}
            SET    payload_json = %s,
                   report_date  = %s,
                   updated_at   = NOW()
            WHERE  id = %s
        """
        insert_sql = f"""
            INSERT INTO {table} (report_date, payload_json)
            VALUES (%s, %s)
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(check_sql, (report_date.year, report_date.month))
                existing = cur.fetchone()

                if existing:
                    row_id   = existing[0]
                    old_date = existing[1]
                    cur.execute(update_sql, (payload_str, report_date, row_id))
                    action = f"UPDATED (was {old_date} -> now {report_date})"
                    action_type = "UPDATED"
                else:
                    cur.execute(insert_sql, (report_date, payload_str))
                    action = f"INSERTED (report_date={report_date})"
                    action_type = "INSERTED"

            self.conn.commit()
            logger.info(f"  [DB] {config['name']} for {report_date.year}-{report_date.month:02d}: {action}")
            return True, action_type

        except Exception as e:
            self.conn.rollback()
            logger.error(f"  [DB] Failed to save {endpoint_code} for {report_date}: {e}")
            return False, str(e)

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------
    def run(
        self,
        mode:          str,
        from_date_str: Optional[str],
        to_date_str:   Optional[str],
        endpoint:      str,
        specific_date: Optional[str] = None,
        truncate:      bool = False,
    ):
        self.connect_db()
        today = date.today()

        # --- Date resolution ---
        if mode == 'current':
            target_dates = [today]
            logger.info(f"Mode: current  ->  target date: {today.isoformat()} ({today.year}-{today.month:02d})")
        elif mode == 'specific':
            if not specific_date:
                logger.error("--date is required for specific mode")
                return
            try:
                d = datetime.strptime(specific_date, '%Y-%m-%d').date()
            except ValueError:
                logger.error(f"Invalid date provided: '{specific_date}'. Please check the calendar (e.g., April only has 30 days).")
                return
            target_dates = [d]
            logger.info(f"Mode: specific ->  target date: {d.isoformat()}")
        else:
            if not from_date_str:
                logger.error("--from-date is required for range mode")
                return
            start_dt     = datetime.strptime(from_date_str, '%Y-%m-%d').date()
            end_dt       = datetime.strptime(to_date_str, '%Y-%m-%d').date() if to_date_str else today
            target_dates = self.get_month_ends(start_dt, end_dt)
            logger.info(f"Mode: range  →  {start_dt} to {end_dt}  ({len(target_dates)} months)")

        if not target_dates:
            logger.info("No valid month-end dates found in the specified range.")
            return

        logger.info(f"Dates to process: {[d.isoformat() for d in target_dates]}")

        # --- Endpoint selection ---
        eps_to_process = [endpoint] if endpoint != 'all' else ['fp', 'bs', 'ia']

        if truncate:
            logger.info(">>> Truncate flag passed. Truncating target tables before load... <<<")
            for ep in eps_to_process:
                config = ENDPOINT_CONFIG[ep]
                table  = config['table']
                try:
                    with self.conn.cursor() as cur:
                        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY;")
                    self.conn.commit()
                    logger.info(f"  [DB] Successfully TRUNCATED table {table} and reset IDs.")
                except Exception as e:
                    self.conn.rollback()
                    logger.error(f"  [DB] Failed to truncate table {table}: {e}")
                    sys.exit(1)

        # Import metadata log utilities here to avoid circular imports if any
        from src.utils.etl_utils import init_metadata_table, start_metadata_log, end_metadata_log
        
        try:
            init_metadata_table()
        except Exception as e:
            logger.warning(f"Could not init metadata table: {e}")

        # --- Extraction loop ---
        errors = []
        for ep in eps_to_process:
            config = ENDPOINT_CONFIG[ep]
            table = config['table']
            logger.info(f"--- Starting Extraction for {config['name']} ---")
            
            run_id = None
            ep_inserted = 0
            ep_updated = 0
            ep_errors = []

            try:
                run_id, _ = start_metadata_log(
                    target_table=table.split('.')[-1],
                    refresh_type='FULL' if truncate else 'INCREMENTAL',
                    load_strategy='UPSERT',
                    source_table=f'1c_api_{ep}',
                    pipeline_name='1c_api_extraction',
                    source_system='1c_api',
                    source_schema=None
                )
            except Exception as e:
                logger.warning(f"Failed to start metadata log: {e}")

            for d in target_dates:
                logger.info(f"\n>>> Processing {d.year}-{d.month:02d} (API date: {d.isoformat()}) <<<")
                api_date_str = d.strftime('%d-%m-%Y')
                path   = f"{config['path']}{api_date_str}"

                logger.info(f"  Fetching {config['name']} ...")
                data = self.fetch_data(path)

                if data:
                    success, action_type = self.save_to_db(ep, data, d)
                    if not success:
                        ep_errors.append(f"DB Save Failed @ {d}")
                        self.send_alert_email("1C API DB Save Failed", f"Failed to save {ep} for date {d} to database.")
                    else:
                        if action_type == "INSERTED": ep_inserted += 1
                        elif action_type == "UPDATED": ep_updated += 1
                else:
                    logger.error(f"  Failed to fetch {ep} for {d}")
                    ep_errors.append(f"API Fetch Failed @ {d}")

            if ep_errors:
                errors.extend([f"{ep}: {e}" for e in ep_errors])
                try:
                    if run_id: end_metadata_log(run_id, 'FAILED', error_message="; ".join(ep_errors))
                except Exception as e: logger.warning(f"Failed to write end metadata log: {e}")
            else:
                try:
                    if run_id: end_metadata_log(run_id, 'SUCCESS', rows_extracted=ep_inserted + ep_updated, rows_inserted=ep_inserted, rows_updated=ep_updated)
                except Exception as e: logger.warning(f"Failed to write end metadata log: {e}")

        self.close_db()

        if errors:
            err_msg = f"\nProcess finished with {len(errors)} error(s): {', '.join(errors)}"
            logger.error(err_msg)
            sys.stderr.write(err_msg + "\n")
            sys.exit(1)
        else:
            logger.info("\nAll tasks completed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description='1C API Data Extraction to Raw Layer',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        '--mode',
        choices=['current', 'range', 'specific'],
        default='current',
        help=(
            "current  : fetch current month (default, no other args needed)\n"
            "specific : fetch a specific date (requires --date)\n"
            "range    : fetch a date range (requires --from-date)"
        ),
    )
    parser.add_argument('--date',      type=str, help='Specific date for specific mode (YYYY-MM-DD)')
    parser.add_argument('--from-date', type=str, help='Start date for range mode (YYYY-MM-DD)')
    parser.add_argument('--to-date',   type=str, help='End date for range mode (YYYY-MM-DD), default=today')
    parser.add_argument(
        '--endpoint',
        choices=['fp', 'bs', 'ia', 'all'],
        default='all',
        help='Which API endpoint to call (fp=FinancialPerformance, bs=BalanceSheet, ia=InvestmentActivity)',
    )
    parser.add_argument('--truncate', action='store_true', help='Truncate the target table(s) before inserting new data')

    args = parser.parse_args()

    extractor = OneCExtractor()
    extractor.run(args.mode, args.from_date, args.to_date, args.endpoint, args.date, args.truncate)


if __name__ == '__main__':
    main()
