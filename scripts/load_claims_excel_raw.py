import os, sys, shutil, logging, re
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

LOG      = os.path.join(BASE_DIR, 'logs', 'watch_excel.log')
DROP_DIR = os.path.join(BASE_DIR, 'excel_drop')
ARCHIVE  = os.path.join(BASE_DIR, 'archive')
ARCHIVE_CLAIMS = os.path.join(ARCHIVE, 'archive_claims')
DROP_CLAIMS_DIR = os.path.join(DROP_DIR, 'Claims_portfolio')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

PG_CONN = (f"host={os.getenv('PG_HOST')} port={os.getenv('PG_PORT', '5432')} "
           f"dbname={os.getenv('PG_DATABASE')} user={os.getenv('PG_USER')} "
           f"password={os.getenv('PG_PASSWORD')}")

# Ensure required folders exist
os.makedirs(DROP_DIR, exist_ok=True)
os.makedirs(ARCHIVE,  exist_ok=True)
os.makedirs(ARCHIVE_CLAIMS, exist_ok=True)
os.makedirs(DROP_CLAIMS_DIR, exist_ok=True)

def detect_type(filename):
    fn = filename.lower()
    if 'claim'   in fn: return 'claims'
    if 'reinsur' in fn: return 'reinsurance'
    if 'solvency' in fn: return 'solvency'
    return None

def deduplicate_columns(cols):
    new_cols = []
    seen = set()
    for c in cols:
        base = c
        name = c
        counter = 1
        while name in seen:
            counter += 1
            name = f"{base}_{counter}"
        seen.add(name)
        new_cols.append(name)
    return new_cols

ENGLISH_MAPPING = {
    'record_id': ['№', 'id', 'п/п'], #A
    'insured_region': ['суғурталанувчи суғурталанган ҳудудий тармоқ'], #B
    'insured_branch': ['суғурталанувчи суғурталанган филиал'], #C
    'reporting_region': ['хабарномани қабул қилган ҳудудий тармоқ'], #D
    'claim_reported_by': ['сугурта ходисаси хакида ариза бер'], #E
    'claim_report_date': ['ариза берилган ёки', 'санаси'], #F
    'grouping_code': ['группировка'], #G
    'insurance_type': ['суғурта тури', 'вид страхования'], #H
    'insurance_class': ['класс'], #I
    'entity_type': ['ю/ф', 'физ/юр'], #J
    'taxpayer_id': ['инн (стир ), юр/лиц'], #K
    'counterparty_name': ['unnamed: 11'], #L Handled by index in code too
    'injured_party': ['жабрланувчи'], #M
    'contract_id': ['шартнома рақами', '№ договор', '№ договора страхования'], #N
    'policy_id': ['полис рақами', 'информация о выплате'], #O
    'incident_date': ['сугурта ходисаси содир булган сана', 'зарар юз берган сана', 'дата и время события'], #P
    'reported_damage_property': ['зарарлар миқдори (мулкка)'], #Q
    'reported_damage_health': ['зарарлар миқдори (согликка)'], #R
    'damage_adjustment_notes': ['зарарларни бартараф', 'маълумот'], #S
    'incident_cause': ['ходисаси сабаби'], #T
    'settlement_order_id': ['қарор тартиб рақами'], #U
    'settlement_decision_date': ['қарор қабул қилинган сана'], #V
    'payout_date': ['қоплама тўланган сана', 'дата выплаты'], #W
    'payout_property': ['мулкка'], #X
    'payout_health': ['согликка'], #Y
    'payout_total': ['жами (19 устун+20 устун)', 'жами (19 устун + 20 устун)', 'сумма убытков'], #Z
    'currency': ['валюта бирлиги'], #AA
    'rejection_date': ['коплама рад этилди сана', 'рад этилган - сана'], #AB
    'rejection_reason': ['рад этиш сабаби', 'причина'], #AC
    'claim_id': ['дело рақами'], #AD
    'claim_expense_amount': ['зарарни бартараф қилиш бўйича харажат (3%)'], #AE
    'outstanding_loss_amount': ['жами бартараф этилмаган зарарлар'], #AF
    'damaged_object': ['зарарланган суғурта объекти'], #AG
    'recovery_amount': ['регресс бўйича ундириладиган сумма', 'регресс миқдори', 'регресс'], #AH
    'claim_handler': ['ижрочи'], #AI
    'notes': ['изоҳ'], #AJ
    'beneficiary': ['наф олувчи'], #AK
    'vehicle_engine_type': ['тв тури(двигатель хажми)'], #AL
    'branch_name': ['филиал', 'отделение'], #AM
    'client_name': ['страхователь', 'заявитель', 'сугурталанувчи'], #AN
    'claim_submission_date': ['келиб тушган сана', 'дата подачи'], #AO
}

FIXED_2021_COLUMNS = [
    'record_id',                # A
    'insured_region',           # B
    'insured_branch',           # C
    'reporting_region',         # D
    'claim_reported_by',        # E
    'claim_report_date',        # F
    'grouping_code',            # G
    'insurance_type',           # H
    'insurance_class',          # I
    'entity_type',              # J
    'taxpayer_id',              # K
    'client_name',              # L (Суғурталанувчи)
    'injured_party',            # M
    'contract_id',              # N
    'policy_id',                # O
    'incident_date',            # P
    'reported_damage_property', # Q
    'reported_damage_health',   # R
    'damage_adjustment_notes',  # S
    'incident_cause',           # T
    'settlement_order_id',      # U
    'settlement_decision_date', # V
    'payout_date',              # W
    'payout_property',          # X
    'payout_health',            # Y
    'payout_total',             # Z
    'currency',                 # AA
    'rejection_date',           # AB 
    'rejection_reason',         # AC
    'claim_id',                 # AD
    'claim_expense_amount',     # AE
    'outstanding_loss_amount',  # AF
    'damaged_object',           # AG
    'recovery_amount',          # AH
    'claim_handler',            # AI
    'notes',                    # AJ
    'beneficiary',              # AK
    'vehicle_engine_type',      # AL
]

def normalize_str(s):
    if not s: return ""
    s = str(s).lower().strip()
    return re.sub(r'[^a-z0-9а-яё]', '', s) # Strip all except alphanumeric for fuzzy match

def find_best_sheet_and_header(xl):
    # Search all sheets for the first one that has markers in the first 20 rows
    for sn in xl.sheet_names:
        try:
            df_preview = pd.read_excel(xl, sheet_name=sn, header=None, nrows=20).fillna('')
            for i, row in df_preview.iterrows():
                row_str = "".join([normalize_str(x) for x in row.values])
                # Check if any keyword matches
                match_count = 0
                for target, kws in ENGLISH_MAPPING.items():
                    if any(normalize_str(kw) in row_str for kw in kws):
                        match_count += 1
                
                if match_count >= 3: # If at least 3 columns match, it's likely our data header
                    return sn, i
        except: continue
    return xl.sheet_names[0], 0 # Fallback

def clean_sql_name(name):
    name = str(name).lower().strip()
    name = re.sub(r'[^a-z0-9а-яё_]', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    # Postgres limit is 63 BYTES. Cyrillic = 2 bytes. 
    # 30 chars = 60 bytes, safe for suffixes.
    return name[:30] if name else 'col_unnamed'

def apply_hybrid_mapping(df):
    new_cols = []
    mapped_targets = {}
    
    # 1. Generate desired names
    for i, col in enumerate(df.columns):
        col_norm = normalize_str(col)
        target_found = None
        
        # Try to find an English target in mapping
        for target, kws in ENGLISH_MAPPING.items():
            if target not in mapped_targets:
                if any(normalize_str(kw) in col_norm for kw in kws):
                    target_found = target
                    mapped_targets[target] = True
                    break
        
        if not target_found:
            # Preserve original under cleaned name
            target_found = clean_sql_name(col)
        
        new_cols.append(target_found)
            
    # 2. Aggressively deduplicate
    final_cols = deduplicate_columns(new_cols)
    
    # 3. Assign and return
    df.columns = final_cols
    return df

def load_excel_bulk(path):
    try:
        with pd.ExcelFile(path, engine='openpyxl') as xl:
            sheet_name, header_row_idx = find_best_sheet_and_header(xl)
            logging.info(f"  Best sheet found: {sheet_name}")
            
            logging.info("  Applying Strict Positional Mapping (5-row header, A-AL limit)")
            df_full = pd.read_excel(xl, sheet_name=sheet_name, header=None)
            
            # Trim to exactly 38 columns (A to AL), starting after row 5
            df_raw = df_full.iloc[5:, :38].copy()
            
            # Pad horizontally if there are somehow fewer than 38 columns natively
            while df_raw.shape[1] < 38:
                df_raw[df_raw.shape[1]] = None
            
            df_raw.columns = FIXED_2021_COLUMNS
            df = df_raw
            
            df['source_file'] = os.path.basename(path)
            df['loaded_at']   = datetime.now().isoformat()
            
            # Drop purely empty rows to stay clean
            df.dropna(how='all', subset=FIXED_2021_COLUMNS, inplace=True)
            
            return df
    except Exception as e:
        logging.error(f"Error bulk loading {path}: {e}")
        return None

def load_claims_portfolio(path, pg):
    try:
        with pd.ExcelFile(path, engine='openpyxl') as xl:
            sheet_name, header_row_idx = find_best_sheet_and_header(xl)
            logging.info(f"  Mapping sheet: {sheet_name}")
            
            logging.info("  Applying Strict Positional Mapping (5-row header, A-AL limit)")
            df_full = pd.read_excel(xl, sheet_name=sheet_name, header=None)
            
            # Trim to exactly 38 columns (A to AL), starting after row 5
            df_raw = df_full.iloc[5:, :38].copy()
            
            # Pad horizontally if there are somehow fewer than 38 columns natively
            while df_raw.shape[1] < 38:
                df_raw[df_raw.shape[1]] = None
            
            df_raw.columns = FIXED_2021_COLUMNS
            df = df_raw
            
            df['source_file'] = f"{os.path.basename(path)} | {sheet_name}"
            df['loaded_at']   = datetime.now().isoformat()
            
            # Drop purely empty rows to stay clean
            df.dropna(how='all', subset=FIXED_2021_COLUMNS, inplace=True)
            
            return df
    except Exception as e:
        logging.error(f"Error loading {path}: {e}")
        return None

def df_to_pg(df, table, pg, schema='raw', append=False):
    if df is None or df.empty: return
    cur = pg.cursor()
    try:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        cols = list(df.columns)
        
        # Log columns safely (avoid unicode crash on print)
        logging.info(f"Ingesting into {schema}.{table} with {len(cols)} columns")
        
        col_defs = ', '.join([f'"{c}" TEXT' for c in cols])
        
        # Check if table exists
        cur.execute(f"SELECT exists(select from information_schema.tables where table_schema=%s and table_name=%s)", (schema, table))
        table_exists = cur.fetchone()[0]

        if not append or not table_exists:
            if not append: cur.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
            cur.execute(f'CREATE TABLE {schema}.{table} ({col_defs})')
        else:
            # Reconciliation: Add missing columns
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (schema, table))
            existing_cols = [r[0] for r in cur.fetchall()]
            for c in cols:
                if c not in existing_cols:
                    cur.execute(f'ALTER TABLE {schema}.{table} ADD COLUMN "{c}" TEXT')

        rows = [tuple(str(v) if not pd.isna(v) else None for v in r) for r in df.itertuples(index=False)]
        if rows:
            columns = ', '.join([f'"{c}"' for c in cols])
            execute_values(cur, f'INSERT INTO {schema}.{table} ({columns}) VALUES %s', rows)
        pg.commit()
    except Exception as e:
        pg.rollback()
        raise e
    finally:
        cur.close()
    logging.info(f'  {schema}.{table}: {len(df)} rows loaded')

def process_file_single(fpath, pg):
    fname = os.path.basename(fpath)
    logging.info(f"Manual loading: {fname}")
    # Use the portfolio loader as it's the most standardized
    df = load_claims_portfolio(fpath, pg)
    if df is not None:
        df_to_pg(df, 'claims_portfolio', pg, schema='raw', append=True)
        logging.info(f"  Done manual ingestion for {fname}")

if __name__ == '__main__':
    logging.info('--- Excel ingestion script started ---')
    pg = psycopg2.connect(PG_CONN)
    
    # Check if a single file was passed via terminal
    if len(sys.argv) > 1:
        target_path = sys.argv[1]
        if os.path.exists(target_path):
            process_file_single(target_path, pg)
        else:
            logging.error(f"File not found: {target_path}")
        pg.close()
        sys.exit()

    # 1. Process main drop folder
    files = [f for f in os.listdir(DROP_DIR) if f.endswith(('.xlsx', '.xls'))]
    for fname in files:
        fpath = os.path.join(DROP_DIR, fname)
        ftype = detect_type(fname)
        if not ftype:
            logging.warning(f'Unknown file type, skipping: {fname}')
            continue
        logging.info(f'Loading {fname} as type={ftype} (bulk)')
        try:
            df = load_excel_bulk(fpath)
            if df is not None:
                df_to_pg(df, f'stg_bulk_{ftype}', pg)
                shutil.move(fpath, os.path.join(ARCHIVE, fname))
                logging.info(f'Archived {fname}')
        except Exception as e:
            pg.rollback() # Ensure rollback on loop error too
            logging.error(f'Failed to load {fname}: {e}')

    # 2. Process Claims_portfolio subfolder
    portfolio_files = [f for f in os.listdir(DROP_CLAIMS_DIR) if f.endswith(('.xlsx', '.xls'))]
    first_p = True
    for fname in portfolio_files:
        fpath = os.path.join(DROP_CLAIMS_DIR, fname)
        logging.info(f'-------------------------------')
        logging.info(f'Loading portfolio file: {fname}')
        try:
            df = load_claims_portfolio(fpath, pg)
            if df is not None:
                # Recreate table on first file of this run, then append
                df_to_pg(df, 'claims_portfolio', pg, schema='raw', append=not first_p)
                first_p = False
                shutil.move(fpath, os.path.join(ARCHIVE_CLAIMS, fname))
                logging.info(f'Archived portfolio file {fname}')
        except Exception as e:
            pg.rollback()
            logging.error(f'Failed to load portfolio {fname}: {e}')

    pg.close()
    logging.info('Excel ingestion done')
