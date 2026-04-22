import os
import sys
import logging
import oracledb
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

# Configure Logging
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'migrate_oracle.log')
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# Configuration
ORACLE_CONFIG = {
    'user': os.getenv('ORACLE_USER'),
    'password': os.getenv('ORACLE_PASS'),
    'dsn': f"{os.getenv('ORACLE_HOST')}:{os.getenv('ORACLE_PORT', 1521)}/{os.getenv('ORACLE_SERVICE')}"
}
oracledb.init_oracle_client = None 
oracledb.defaults.fetch_lobs = False # Force CLOB/BLOB to fetch as strings/bytes

PG_CONFIG = (
    f"host={os.getenv('PG_HOST')} "
    f"port={os.getenv('PG_PORT', '5432')} "
    f"dbname={os.getenv('PG_DATABASE')} "
    f"user={os.getenv('PG_USER')} "
    f"password={os.getenv('PG_PASSWORD')}"
)

ORACLE_SCHEMA = 'KAPITALDB'
TABLES_TO_MIGRATE = [
    'ins_kurs',
    'ins_anketa',
    'ins_polis',
    'ins_bank_client',
    'ins_kontragent',
    'ins_oplata',
    'ins_agent_akt',
    'tb_anketa',
    'tb_polis',
    'tb_oplata',
    'tb_avto',
    'INS_INVDEP',
    'P_SP_CURRENCY',
    'INS_INVLOAN',
    'SP_ORGTYPE',
    'INS_INVLOAN_OPLATA',
    'INS_INVLOAN_ACCRUAL',
    'TB_USERS'
]

BATCH_SIZE = 10000

def map_oracle_to_pg_type(ora_type, precision, scale):
    ora_type = ora_type.upper()
    if ora_type == 'NUMBER':
        if precision is None and scale is None:
            return 'NUMERIC' # Arbitrary precision to avoid truncating decimals
        if scale == 0 and precision is not None:
            if precision < 10: 
                return 'INTEGER'
            elif precision <= 18: 
                return 'BIGINT'
            else: 
                return 'NUMERIC'
        return f'NUMERIC({precision or 38}, {scale if scale is not None else 0})'
    elif ora_type in ('VARCHAR2', 'CHAR', 'CLOB', 'NVARCHAR2'):
        return 'VARCHAR' if ora_type != 'CLOB' else 'TEXT'
    elif ora_type in ('DATE', 'TIMESTAMP'):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def get_oracle_columns(ora_cursor, table_name):
    query = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = :table_name AND OWNER = :owner
        ORDER BY COLUMN_ID
    """
    ora_cursor.execute(query, table_name=table_name.upper(), owner=ORACLE_SCHEMA)
    return ora_cursor.fetchall()

def get_counts(ora_cursor, pg_cursor, ora_table, pg_table):
    ora_cursor.execute(f"SELECT COUNT(*) FROM {ORACLE_SCHEMA}.{ora_table}")
    o_count = ora_cursor.fetchone()[0]
    
    pg_cursor.execute(f"SELECT COUNT(*) FROM raw.{pg_table}")
    p_count = pg_cursor.fetchone()[0]
    return o_count, p_count


def migrate_table(ora_conn, pg_conn, table_name):
    # PostgreSQL stores unquoted identifiers in lowercase
    pg_table = f"{table_name}_oracle".lower()
    ora_cursor = ora_conn.cursor()
    pg_cursor = pg_conn.cursor()
    
    try:
        # 1. Inspect Schema
        columns_meta = get_oracle_columns(ora_cursor, table_name)
        if not columns_meta:
            logging.error(f"{table_name}: Not found in Oracle schema {ORACLE_SCHEMA}")
            return
        
        col_names = [c[0].lower() for c in columns_meta]
        date_cols = [c[0] for c in columns_meta if 'DATE' in c[1].upper() or 'TIMESTAMP' in c[1].upper()]
        mod_col = next((c for c in col_names if 'modified' in c or 'updated' in c), None)

        # 2. Check existence and validation
        pg_cursor.execute("SELECT exists(select from information_schema.tables where table_schema='raw' and table_name=%s)", (pg_table,))
        exists = pg_cursor.fetchone()[0]
        
        decision = "CREATED"
        o_count, p_count = 0, 0
        o_max, p_max = None, None

        if exists:
            # Guarantee the ETL columns exist on previously created tables
            pg_cursor.execute(f"ALTER TABLE raw.{pg_table} ADD COLUMN IF NOT EXISTS etl_uploaded_date TIMESTAMP;")
            pg_cursor.execute(f"ALTER TABLE raw.{pg_table} ADD COLUMN IF NOT EXISTS etl_updated_date TIMESTAMP;")
            pg_cursor.execute(f"SELECT COUNT(*), MAX(etl_updated_date) FROM raw.{pg_table}")
            _res = pg_cursor.fetchone()
            p_count = _res[0]
            pg_max_etl = _res[1]
            p_max = pg_max_etl
            
            if mod_col:
                # Fast — Oracle MAX on an indexed date column is cheap
                ora_cursor.execute(f"SELECT MAX({mod_col}) FROM {ORACLE_SCHEMA}.{table_name}")
                o_max = ora_cursor.fetchone()[0]
                
                # Reload if never loaded or source has newer data
                if pg_max_etl is None or (o_max is not None and pg_max_etl < o_max):
                    decision = "RELOADED"
                else:
                    decision = "SKIPPED"
            else:
                # Fallback: skip if etl_updated_date was set within last 24h
                from datetime import timedelta
                timespan_threshold = datetime.now() - timedelta(days=1)
                if pg_max_etl is None or pg_max_etl < timespan_threshold:
                    decision = "RELOADED"
                else:
                    decision = "SKIPPED"
            
            if decision == "SKIPPED":
                logging.info(f"{table_name:25} | PG:{p_count:9} | Ora MaxDate: {str(o_max):<24} | ETL Updated: {str(p_max):<24} | {decision}")
                return

        # 3. Perform Migration (RELOAD or CREATE)
        logging.info(f"{table_name:25} | PG:{p_count:9} | Ora MaxDate: {str(o_max):<24} | ETL Updated: {str(p_max):<24} | {decision}")
        
        # Create table if not exists
        pg_column_defs = []
        for col in columns_meta:
            pg_type = map_oracle_to_pg_type(col[1], col[2], col[3])
            pg_column_defs.append(f"{col[0].lower()} {pg_type} {'NULL' if col[4]=='Y' else 'NOT NULL'}")
        
        # Add ETL timestamp columns (etl_uploaded_date = first load, etl_updated_date = every reload)
        pg_column_defs.append("etl_uploaded_date TIMESTAMP NULL")
        pg_column_defs.append("etl_updated_date TIMESTAMP NULL")
        
        pg_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS raw;")
        pg_cursor.execute(f"CREATE TABLE IF NOT EXISTS raw.{pg_table} ({', '.join(pg_column_defs)})")
        
        # Capture the original uploaded_date before truncating (for RELOAD case)
        original_uploaded_date = None
        if decision == "RELOADED":
            pg_cursor.execute(f"SELECT MIN(etl_uploaded_date) FROM raw.{pg_table}")
            original_uploaded_date = pg_cursor.fetchone()[0]
        
        # Truncate for reload
        pg_cursor.execute(f"TRUNCATE TABLE raw.{pg_table}")
        
        now = datetime.now()
        uploaded_date = original_uploaded_date if decision == "RELOADED" else now
        
        # Batch Move — include ETL timestamps in each row
        ora_cursor.execute(f"SELECT {', '.join(col_names)} FROM {ORACLE_SCHEMA}.{table_name}")
        insert_cols = ', '.join(col_names) + ', etl_uploaded_date, etl_updated_date'
        insert_sql = f"INSERT INTO raw.{pg_table} ({insert_cols}) VALUES %s"
        
        inserted = 0
        while True:
            rows = ora_cursor.fetchmany(BATCH_SIZE)
            if not rows: break
            # Append ETL timestamps to each row
            rows_with_etl = [r + (uploaded_date, now) for r in rows]
            extras.execute_values(pg_cursor, insert_sql, rows_with_etl)
            inserted += len(rows)
            if inserted % (BATCH_SIZE * 5) == 0:  # Log every 50k rows
                logging.info(f"  ... {inserted} rows migrated so far")
        
        pg_conn.commit()
        logging.info(f"  Successfully {decision} {inserted} rows.")

    except (Exception, KeyboardInterrupt) as e:
        pg_conn.rollback()
        logging.error(f"  {table_name} ABORTED/FAILED: {e}")
        if isinstance(e, KeyboardInterrupt):
            logging.warning("  Detected User Interrupt. Rolling back current table transaction...")
            raise e # Re-raise to stop the whole script if needed
    finally:
        ora_cursor.close()
        pg_cursor.close()

if __name__ == '__main__':
    logging.info(f"{'Table':<25} | {'PG Rows':>9} | {'Ora MaxDate':<36} | {'ETL Updated':<24} | Decision")
    logging.info("-" * 115)
    ora, pg = None, None
    try:
        ora = oracledb.connect(**ORACLE_CONFIG)
        pg = psycopg2.connect(PG_CONFIG)
        for t in TABLES_TO_MIGRATE:
            migrate_table(ora, pg, t)
    except Exception as e:
        logging.error(f"Migration error: {e}")
    finally:
        if ora: ora.close()
        if pg: pg.close()
        logging.info("Connections closed successfully.")

    """
CONVERTED POSTGRESQL QUERY:

SELECT
    SUM(CASE WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
        ELSE COALESCE(o.opl_summa, 0) * raw.F_INS_GETKURS(o.opl_val, o.opl_data) END) AS OPLSUM,
    SUM(CASE WHEN o.opl_val = 1 THEN COALESCE(a.ins_otv, 0)
        ELSE COALESCE(a.ins_otv, 0) * raw.F_INS_GETKURS(o.opl_val, o.opl_data) END) AS INS_OTV,
    SUM(a.ins_otv) AS INS_OTV2
FROM raw.ins_oplata_oracle o
LEFT JOIN raw.ins_anketa_oracle a ON a.ins_id = o.anketa_id
LEFT JOIN raw.ins_polis_oracle po ON po.tb_id = o.polis_id
LEFT JOIN raw.ins_bank_client_oracle bc ON o.bc_id = bc.ins_id
LEFT JOIN raw.ins_agent_akt_oracle akt ON akt.ins_id = o.akt AND akt.active = 2
LEFT JOIN raw.ins_kontragent_oracle k ON a.owner = k.tb_id
WHERE o.ins_type <> 3
AND EXISTS (
    SELECT 1 FROM raw.ins_polis_oracle p
    WHERE p.tb_status IN (2, 9, 10) AND p.tb_anketa = o.anketa_id
)
AND bc.PYM_DATE >= '2025-01-01'::DATE
AND bc.PYM_DATE < '2025-04-01'::DATE;
"""
