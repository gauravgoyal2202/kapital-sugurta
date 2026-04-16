import os, sys, json, logging, http.client, base64, time, calendar, argparse
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, date

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

LOG = os.path.join(BASE_DIR, 'logs', 'extract_1c.log')
OUT = os.path.join(BASE_DIR, 'staging', 'json')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)

def get_last_days(start_year=2021, start_month=1):
    """Generates the last day of every month from start date to today."""
    current_date = datetime.now()
    dates = []
    for year in range(start_year, current_date.year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == current_date.year and month > current_date.month:
                break
            
            last_day = calendar.monthrange(year, month)[1]
            dates.append(date(year, month, last_day))
    return dates

# ── Connection details ───────────────────────────────────────────────────────
API_HOST = os.getenv('API_1C_HOST', '10.10.1.209').replace('http://', '').replace('https://', '').rstrip('/')
API_USER = os.getenv('API_1C_USER')
API_PASS = os.getenv('API_1C_PASS')

_token  = base64.b64encode(f'{API_USER}:{API_PASS}'.encode()).decode()
HEADERS = {'Authorization': f'Basic {_token}'}

PG_CONN = (f"host={os.getenv('PG_HOST')} port={os.getenv('PG_PORT', '5432')} "
           f"dbname={os.getenv('PG_DATABASE')} user={os.getenv('PG_USER')} "
           f"password={os.getenv('PG_PASSWORD')}")

# ── Raw table DDL ────────────────────────────────────────────────────────────
SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

-- 1. Financial Performance: one row per F-code per report_date
-- CREATE TABLE IF NOT EXISTS raw.financial_performance (
--    id          BIGSERIAL PRIMARY KEY,
--    report_date DATE NOT NULL,
--    code        VARCHAR(20) NOT NULL,
--    amount      NUMERIC(22,4),
--    fetched_at  TIMESTAMP DEFAULT NOW(),
--    UNIQUE (report_date, code)
-- );

-- 2. Balance Sheet: one row per A/P-code per report_date
-- CREATE TABLE IF NOT EXISTS raw.balance_sheet (
--    id          BIGSERIAL PRIMARY KEY,
--    report_date DATE NOT NULL,
--    code        VARCHAR(20) NOT NULL,
--    amount      NUMERIC(22,4),
--    fetched_at  TIMESTAMP DEFAULT NOW(),
--    UNIQUE (report_date, code)
-- );

-- 3. Investment Activity: one row per investment line item
-- CREATE TABLE IF NOT EXISTS raw.investment_activity (
--    id          BIGSERIAL PRIMARY KEY,
--    report_date DATE NOT NULL,
--    category    VARCHAR(50) NOT NULL,
--    partner     TEXT,
--    contract    TEXT,
--    amount      NUMERIC(22,4),
--    fetched_at  TIMESTAMP DEFAULT NOW()
-- );

-- 4a. Full JSON — Financial Performance Report
CREATE TABLE IF NOT EXISTS raw.financial_performance_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW()
);

-- 4b. Full JSON — Balance Sheet Report
CREATE TABLE IF NOT EXISTS raw.balance_sheet_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW()
);

-- 4c. Full JSON — Investment Activity Report
CREATE TABLE IF NOT EXISTS raw.investment_activity_api_response (
    id           BIGSERIAL PRIMARY KEY,
    report_date  DATE      NOT NULL UNIQUE,
    payload_json JSONB     NOT NULL,
    ingested_at  TIMESTAMP DEFAULT NOW()
);
"""

# Map endpoint short code -> (table_name, api_path_template)
ENDPOINT_CONFIG = {
    'fp': ('raw.financial_performance_api_response', '/kapital/hs/api/Report/FinancialPerformance?Date='),
    'bs': ('raw.balance_sheet_api_response', '/kapital/hs/api/Report/BalanceSheet?Date='),
    'ia': ('raw.investment_activity_api_response', '/kapital/hs/api/Report/InvestmentActivity?Date='),
}

# Map endpoint short code -> (table_name, full report name)
ENDPOINT_META = {
    'fp': ('raw.financial_performance_api_response', 'Financial Performance Report'),
    'bs': ('raw.balance_sheet_api_response', 'Balance Sheet Report'),
    'ia': ('raw.investment_activity_api_response', 'Investment Activity Report'),
}


# ── HTTP fetch ───────────────────────────────────────────────────────────────
def fetch_json(path, retries=3):
    for attempt in range(1, retries + 1):
        try:
            conn = http.client.HTTPConnection(API_HOST, timeout=60)
            conn.request('GET', path, '', HEADERS)
            res = conn.getresponse()
            raw = res.read()
            conn.close()
            if res.status != 200:
                raise ValueError(f'HTTP {res.status} {res.reason}')
            return json.loads(raw.decode('utf-8'))
        except Exception as e:
            logging.warning(f'Attempt {attempt} failed for {path}: {e}')
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None

# ── Save raw JSON blob ───────────────────────────────────────────────────────
def save_raw_json(cur, endpoint_code, data, report_date):
    """Upsert the full JSON response into its dedicated raw table."""
    table_name, _ = ENDPOINT_CONFIG.get(endpoint_code)
    cur.execute(f"""
        INSERT INTO {table_name} (report_date, payload_json)
        VALUES (%s, %s)
        ON CONFLICT (report_date)
        DO UPDATE SET payload_json = EXCLUDED.payload_json,
                      ingested_at  = NOW()
    """, (report_date, json.dumps(data, ensure_ascii=False)))
    logging.info(f'  JSON blob saved to {table_name}')

# ── Parsers ──────────────────────────────────────────────────────────────────
def parse_fp(data, report_date):
    """FinancialPerformance → list of (report_date, code, amount)"""
    rows = []
    payload = data.get('OperationResult', {})
    for code, val in payload.items():
        if code == 'Result':
            continue
        if isinstance(val, (int, float)):
            rows.append((report_date, code, float(val)))
    return rows

def parse_bs(data, report_date):
    """BalanceSheet → list of (report_date, code, amount)"""
    rows = []
    balance = data.get('OperationResult', {}).get('Balance', {})
    for code, val in balance.items():
        if isinstance(val, (int, float)):
            rows.append((report_date, code, float(val)))
    return rows

def parse_ia(data, report_date):
    """InvestmentActivity → list of (report_date, category, partner, contract, amount)"""
    rows = []
    payload = data.get('OperationResult', {})
    for key, val in payload.items():
        if key == 'Result':
            continue
        if isinstance(val, (int, float)):
            # Scalar entry like Loans / Shares
            rows.append((report_date, key, None, None, float(val)))
        elif isinstance(val, list):
            # Array of {partner, contract, amount}
            for item in val:
                rows.append((
                    report_date,
                    key,
                    item.get('partner'),
                    item.get('contract'),
                    float(item.get('amount', 0))
                ))
    return rows

# ── DB loaders ───────────────────────────────────────────────────────────────
def load_fp(cur, rows, report_date):
    cur.executemany("""
        INSERT INTO raw.financial_performance (report_date, code, amount)
        VALUES (%s, %s, %s)
        ON CONFLICT (report_date, code)
        DO UPDATE SET amount=EXCLUDED.amount, fetched_at=NOW()
    """, rows)
    logging.info(f'  FP: {len(rows)} codes upserted')

def load_bs(cur, rows, report_date):
    cur.executemany("""
        INSERT INTO raw.balance_sheet (report_date, code, amount)
        VALUES (%s, %s, %s)
        ON CONFLICT (report_date, code)
        DO UPDATE SET amount=EXCLUDED.amount, fetched_at=NOW()
    """, rows)
    logging.info(f'  BS: {len(rows)} codes upserted')

def load_ia(cur, rows, report_date):
    # Delete today's rows first so re-runs don't duplicate array items
    cur.execute("DELETE FROM raw.investment_activity WHERE report_date=%s", (report_date,))
    cur.executemany("""
        INSERT INTO raw.investment_activity
            (report_date, category, partner, contract, amount)
        VALUES (%s, %s, %s, %s, %s)
    """, rows)
    logging.info(f'  IA: {len(rows)} rows inserted')

# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='1C API Extraction Script')
    parser.add_argument('--mode', choices=['historical', 'current'], default='current',
                      help='Run mode: historical (Jan 2021-now) or current (this month)')
    parser.add_argument('--table', choices=['fp', 'bs', 'ia', 'all'], default='all',
                      help='Target table/endpoint to process')
    args = parser.parse_args()

    logging.info(f'=== 1C Extraction started (Mode: {args.mode}, Table: {args.table}) ===')

    # 1. Ensure raw tables exist
    try:
        pg = psycopg2.connect(PG_CONN)
        pg.autocommit = True
        pg.cursor().execute(SETUP_SQL)
        pg.autocommit = False
        logging.info('Raw schema ready')
    except Exception as e:
        logging.error(f'DB setup failed: {e}')
        sys.exit(1)

    # 2. Determine target dates
    if args.mode == 'historical':
        target_dates = get_last_days()
    else:
        # Just the last day of the current month
        today = datetime.now()
        last_day = calendar.monthrange(today.year, today.month)[1]
        target_dates = [date(today.year, today.month, last_day)]

    logging.info(f'Target dates: {[d.strftime("%Y-%m-%d") for d in target_dates]}')

    # 3. Filter jobs
    all_jobs = [
        ('fp', parse_fp, load_fp),
        ('bs', parse_bs, load_bs),
        ('ia', parse_ia, load_ia),
    ]
    
    if args.table == 'all':
        jobs = all_jobs
    else:
        jobs = [j for j in all_jobs if j[0] == args.table]

    errors = []
    cur = pg.cursor()

    for d in target_dates:
        date_str = d.strftime('%d-%m-%Y')
        logging.info(f'>>> Processing Date: {date_str} <<<')

        for name, parser_func, loader_func in jobs:
            _, path_root = ENDPOINT_CONFIG[name]
            path = path_root + date_str
            
            logging.info(f'--- {name} @ {date_str} ---')
            data = fetch_json(path)
            if data is None:
                logging.error(f'  {name}: fetch failed')
                errors.append(f'{name}@{date_str}')
                continue

            # 1. Save JSON backup
            out_path = os.path.join(OUT, f'{name}_{date_str}.json')
            try:
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logging.warning(f'  Could not save JSON file: {e}')

            # 2. Store full JSON blob
            try:
                save_raw_json(cur, name, data, d)
                pg.commit()
            except Exception as e:
                pg.rollback()
                logging.error(f'  {name} JSON blob fail: {e}')

            # 3. Parse and insert row-level
            # try:
            #     rows = parser_func(data, d)
            #     loader_func(cur, rows, d)
            #     pg.commit()
            # except Exception as e:
            #     pg.rollback()
            #     logging.error(f'  {name} row-level fail: {e}')
            #     errors.append(f'{name}@{date_str}')

    cur.close()
    pg.close()

    if errors:
        logging.error(f'Failed tasks: {len(errors)}')
    logging.info(f'=== 1C Extraction complete ({args.mode}) ===')
