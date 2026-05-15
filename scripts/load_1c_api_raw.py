import os
import sys
import json
import logging
import time
import calendar
import argparse
from datetime import datetime, date
from typing import List, Optional, Dict, Any

import requests
import psycopg2
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# --- Configuration & Setup ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'extract_1c.log')

STAGING_DIR = os.path.join(BASE_DIR, 'staging', 'json')
os.makedirs(STAGING_DIR, exist_ok=True)

# Industry Standard Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger('1C_Extractor')

class Config:
    API_HOST = os.getenv('API_1C_HOST', '10.10.1.209').replace('http://', '').replace('https://', '').rstrip('/')
    API_USER = os.getenv('API_1C_USER')
    API_PASS = os.getenv('API_1C_PASS')
    
    PG_HOST = os.getenv('PG_HOST')
    PG_PORT = os.getenv('PG_PORT', '5432')
    PG_DATABASE = os.getenv('PG_DATABASE')
    PG_USER = os.getenv('PG_USER')
    PG_PASSWORD = os.getenv('PG_PASSWORD')
    
    @classmethod
    def validate(cls):
        missing = []
        if not cls.API_USER: missing.append('API_1C_USER')
        if not cls.API_PASS: missing.append('API_1C_PASS')
        if not cls.PG_DATABASE: missing.append('PG_DATABASE')
        if missing:
            logger.error(f"Missing environment variables: {', '.join(missing)}")
            sys.exit(1)

    @classmethod
    def get_pg_conn_str(cls):
        return f"host={cls.PG_HOST} port={cls.PG_PORT} dbname={cls.PG_DATABASE} user={cls.PG_USER} password={cls.PG_PASSWORD}"

# --- Constants & SQL ---
SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.financial_performance_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.balance_sheet_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.investment_activity_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW()
);
"""

ENDPOINT_CONFIG = {
    'fp': {
        'table': 'raw.financial_performance_api_response',
        'path': '/kapital/hs/api/Report/FinancialPerformance?Date=',
        'name': 'Financial Performance'
    },
    'bs': {
        'table': 'raw.balance_sheet_api_response',
        'path': '/kapital/hs/api/Report/BalanceSheet?Date=',
        'name': 'Balance Sheet'
    },
    'ia': {
        'table': 'raw.investment_activity_api_response',
        'path': '/kapital/hs/api/Report/InvestmentActivity?Date=',
        'name': 'Investment Activity'
    },
}

class OneCExtractor:
    def __init__(self):
        Config.validate()
        self.auth = HTTPBasicAuth(Config.API_USER, Config.API_PASS)
        self.base_url = f"http://{Config.API_HOST}"
        self.session = requests.Session()
        self.conn = None

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(Config.get_pg_conn_str())
            with self.conn.cursor() as cur:
                cur.execute(SETUP_SQL)
            self.conn.commit()
            logger.info("Database connection established and schema verified.")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            sys.exit(1)

    def close_db(self):
        if self.conn:
            self.conn.close()

    def get_month_ends(self, start_date: date, end_date: date) -> List[date]:
        curr = start_date
        month_ends = []
        while curr <= end_date:
            last_day = calendar.monthrange(curr.year, curr.month)[1]
            dt = date(curr.year, curr.month, last_day)
            if start_date <= dt <= end_date:
                if dt not in month_ends:
                    month_ends.append(dt)
            # Move to next month
            if curr.month == 12:
                curr = date(curr.year + 1, 1, 1)
            else:
                curr = date(curr.year, curr.month + 1, 1)
        return month_ends

    def fetch_data(self, path: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{path}"
        for attempt in range(1, retries + 1):
            try:
                response = self.session.get(url, auth=self.auth, timeout=60)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP Error {response.status_code} for {url}: {e}")
                if response.status_code == 401:
                    logger.error("Authentication failed. Check credentials.")
                    return None
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed for {url}: {e}")
            
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def save_to_db(self, endpoint_code: str, data: Dict[str, Any], report_date: date) -> bool:
        config = ENDPOINT_CONFIG[endpoint_code]
        table = config['table']
        try:
            with self.conn.cursor() as cur:
                # Idempotent logic: Overwrite existing record for the same date
                cur.execute(f"DELETE FROM {table} WHERE report_date = %s", (report_date,))
                cur.execute(f"""
                    INSERT INTO {table} (report_date, payload_json)
                    VALUES (%s, %s)
                """, (report_date, json.dumps(data, ensure_ascii=False)))
            self.conn.commit()
            logger.info(f"  [DB] Successfully saved {config['name']} for {report_date}")
            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"  [DB] Failed to save {endpoint_code} for {report_date}: {e}")
            return False

    def save_to_file(self, endpoint_code: str, data: Dict[str, Any], report_date: date):
        filename = f"{endpoint_code}_{report_date.strftime('%Y-%m-%d')}.json"
        filepath = os.path.join(STAGING_DIR, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"  [FILE] Could not backup JSON to {filepath}: {e}")

    def run(self, mode: str, from_date_str: Optional[str], to_date_str: Optional[str], endpoint: str):
        self.connect_db()
        today = date.today()
        
        # 1. Date Logic
        if mode == 'current':
            last_day = calendar.monthrange(today.year, today.month)[1]
            target_dates = [date(today.year, today.month, last_day)]
        else:
            if not from_date_str:
                logger.error("--from-date is required for range mode")
                return
            start_dt = datetime.strptime(from_date_str, '%Y-%m-%d').date()
            end_dt = datetime.strptime(to_date_str, '%Y-%m-%d').date() if to_date_str else today
            target_dates = self.get_month_ends(start_dt, end_dt)

        if not target_dates:
            logger.info("No valid month-end dates found in the specified range.")
            return

        logger.info(f"Targeting {len(target_dates)} dates: {[d.isoformat() for d in target_dates]}")

        # 2. Endpoint Selection
        eps_to_process = [endpoint] if endpoint != 'all' else ['fp', 'bs', 'ia']
        
        # 3. Extraction Loop
        errors = []
        for d in target_dates:
            logger.info(f"\n>>> Processing Date: {d.isoformat()} <<<")
            api_date_str = d.strftime('%d-%m-%Y')
            
            for ep in eps_to_process:
                config = ENDPOINT_CONFIG[ep]
                path = f"{config['path']}{api_date_str}"
                
                logger.info(f"  Fetching {config['name']}...")
                data = self.fetch_data(path)
                
                if data:
                    self.save_to_file(ep, data, d)
                    if not self.save_to_db(ep, data, d):
                        errors.append(f"{ep}@{d}")
                else:
                    logger.error(f"  Failed to fetch {ep} for {d}")
                    errors.append(f"{ep}@{d}")

        self.close_db()
        
        if errors:
            logger.error(f"\nProcess finished with {len(errors)} errors: {', '.join(errors)}")
        else:
            logger.info("\nAll tasks completed successfully.")

def main():
    parser = argparse.ArgumentParser(description='1C API Data Extraction to Raw Layer')
    parser.add_argument('--mode', choices=['current', 'range'], default='current', help='Run mode')
    parser.add_argument('--from-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--to-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--endpoint', choices=['fp', 'bs', 'ia', 'all'], default='all', help='Target endpoint')
    
    args = parser.parse_args()
    
    extractor = OneCExtractor()
    extractor.run(args.mode, args.from_date, args.to_date, args.endpoint)

if __name__ == '__main__':
    main()
