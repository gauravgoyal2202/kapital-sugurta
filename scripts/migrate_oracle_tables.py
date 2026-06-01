import os
import sys
import logging
import oracledb
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
from datetime import datetime
import time
import traceback

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
    'INS_REGRESS',
    'INS_REGRESS_BANK',
    'INS_HEADBANKS',
    'INS_RASTORG',
    'ins_anketa',
    'ins_polis',
    'ins_bank_client',
    'ins_kontragent',
    'ins_oplata',
    'tb_anketa',
    'tb_polis',
    'tb_oplata',
    'tb_avto',
    'ins_agent_akt',
    'ins_kurs',
    'INS_INVDEP',
    'P_SP_CURRENCY',
    'INS_INVLOAN',
    'SP_ORGTYPE',
    'INS_INVLOAN_OPLATA',
    'INS_INVLOAN_ACCRUAL',
    'TB_USERS',
    'INS_PTURI',
    'INS_SOBITIE',
    'INS_VIPLATI',
    'INS_EMPLOYEE',
    'ins_reinsurance',  
    'sp_reinsurance_form',
    'sp_reinsurance_type',
    'sp_reinsurance_brokers',
    'ins_reins_contract',
    'sp_division',
    'sp_reinsurance_org',
    'sp_reinsurance_foreign_org',
    'sp_country',
    'INS_BANK_PTURI',
    'INS_OSGO'  
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

def get_oracle_pks(ora_cursor, table_name):
    query = """
        SELECT cols.column_name
        FROM all_constraints cons, all_cons_columns cols
        WHERE cols.table_name = :table_name
        AND cons.constraint_type = 'P'
        AND cons.constraint_name = cols.constraint_name
        AND cons.owner = :owner
        ORDER BY cols.position
    """
    ora_cursor.execute(query, table_name=table_name.upper(), owner=ORACLE_SCHEMA)
    return [row[0].lower() for row in ora_cursor.fetchall()]

INCREMENTAL_CONFIG = {
    'INS_AGENT_AKT': 'MODIFIED_DATE',
    'ins_anketa': 'MODIFIED_DATE',
    'ins_bank_client': 'MODIFIED_DATE',
    'INS_EMPLOYEE': 'POST_DATE',
    'INS_HEADBANKS': 'INS_ID',
    'INS_INVDEP': 'DOG_DATE',
    'INS_INVLOAN': 'DOG_DATE',
    'INS_INVLOAN_ACCRUAL': 'ACCDATE_FROM',
    'INS_INVLOAN_OPLATA': 'INS_ID',
    'ins_kontragent': 'MODIFIED_DATE',
    'ins_kurs': 'KURS_DATE',
    'ins_oplata': 'MODIFIED_DATE',
    'ins_polis': 'MODIFIED_DATE',
    'INS_PTURI': 'MODIFIED_DATE',
    'INS_RASTORG': 'MODIFIED_DATE',
    'INS_REGRESS': 'CREATE_DATE',
    'INS_REGRESS_BANK': 'MODIFIED_DATE',
    'ins_reinsurance': 'CREATED_DATE',
    'ins_reins_contract': 'CONTRACT_ISSUE_DATE',
    'INS_SOBITIE': 'MODIFIED_DATE',
    'INS_VIPLATI': 'MODIFIED_DATE',
    'P_SP_CURRENCY': 'SP_ID',
    'sp_country': 'SP_DATE',
    'sp_division': 'SP_CREATED_DATE',
    'SP_ORGTYPE': 'SP_ID',
    'sp_reinsurance_brokers': 'COUNTRY_ID',
    'sp_reinsurance_foreign_org': 'MODIFIED_DATE',
    'tb_anketa': 'TB_PASPDATE',
    'tb_avto': 'TB_TEXPDATE',
    'tb_oplata': 'TB_DATEOPL',
    'tb_polis': 'TB_DATE_BEGIN',
    'TB_USERS': 'TB_VIDANDATE',
    'INS_BANK_PTURI': 'CREATED_DATE'
}

def init_watermark_table(pg_cursor):
    pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.etl_watermarks (
            table_name VARCHAR(255) PRIMARY KEY,
            last_watermark_value TEXT,
            last_watermark_type VARCHAR(50),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

def get_watermark(pg_cursor, table_name):
    pg_cursor.execute("SELECT last_watermark_value FROM raw.etl_watermarks WHERE table_name = %s", (table_name.lower(),))
    res = pg_cursor.fetchone()
    return res[0] if res else None

def update_watermark(pg_cursor, table_name, value, w_type):
    if value is None: return
    # Format datetime robustly for storage
    if isinstance(value, datetime):
        val_str = value.strftime('%Y-%m-%d %H:%M:%S.%f')
    else:
        val_str = str(value)
        
    pg_cursor.execute("""
        INSERT INTO raw.etl_watermarks (table_name, last_watermark_value, last_watermark_type, updated_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (table_name) DO UPDATE SET
            last_watermark_value = EXCLUDED.last_watermark_value,
            last_watermark_type = EXCLUDED.last_watermark_type,
            updated_at = EXCLUDED.updated_at;
    """, (table_name.lower(), val_str, w_type))

def get_counts(ora_cursor, pg_cursor, ora_table, pg_table):
    ora_cursor.execute(f"SELECT COUNT(*) FROM {ORACLE_SCHEMA}.{ora_table}")
    o_count = ora_cursor.fetchone()[0]
    
    pg_cursor.execute(f"SELECT COUNT(*) FROM raw.{pg_table}")
    p_count = pg_cursor.fetchone()[0]
    return o_count, p_count


def migrate_table(ora_conn, pg_conn, table_name):
    start_time = time.time()
    # PostgreSQL stores unquoted identifiers in lowercase
    pg_table = f"{table_name}_oracle".lower()
    ora_cursor = ora_conn.cursor()
    pg_cursor = pg_conn.cursor()
    
    result = {
        'table': table_name,
        'status': 'PENDING',
        'rows': 0,
        'duration': 0,
        'decision': 'N/A',
        'error': None
    }
    
    try:
        # 0. Ensure watermark table exists
        init_watermark_table(pg_cursor)
        
        # 1. Inspect Schema
        columns_meta = get_oracle_columns(ora_cursor, table_name)
        if not columns_meta:
            logging.error(f"{table_name}: Not found in Oracle schema {ORACLE_SCHEMA}")
            result['status'] = 'NOT FOUND'
            return result
        
        col_names = [c[0].lower() for c in columns_meta]
        inc_col = INCREMENTAL_CONFIG.get(table_name) or INCREMENTAL_CONFIG.get(table_name.upper()) or INCREMENTAL_CONFIG.get(table_name.lower())
        pk_cols = get_oracle_pks(ora_cursor, table_name)
        
        # 2. Synchronize Schema
        pg_cursor.execute("SELECT exists(select from information_schema.tables where table_schema='raw' and table_name=%s)", (pg_table,))
        exists = pg_cursor.fetchone()[0]
        
        if not exists:
            pg_column_defs = []
            for col in columns_meta:
                pg_type = map_oracle_to_pg_type(col[1], col[2], col[3])
                pg_column_defs.append(f"{col[0].lower()} {pg_type} {'NULL' if col[4]=='Y' else 'NOT NULL'}")
            
            pg_column_defs.append("etl_uploaded_date TIMESTAMP NULL")
            pg_column_defs.append("etl_updated_date TIMESTAMP NULL")
            
            pg_cursor.execute(f"CREATE TABLE raw.{pg_table} ({', '.join(pg_column_defs)})")
            if pk_cols:
                pg_cursor.execute(f"ALTER TABLE raw.{pg_table} ADD PRIMARY KEY ({', '.join(pk_cols)})")
            logging.info(f"  [{table_name}] Created target table raw.{pg_table}")
        else:
            # Sync existing columns
            pg_cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='raw' AND table_name=%s", (pg_table,))
            current_pg_cols = {row[0].lower() for row in pg_cursor.fetchall()}
            for col in columns_meta:
                c_name = col[0].lower()
                if c_name not in current_pg_cols:
                    pg_type = map_oracle_to_pg_type(col[1], col[2], col[3])
                    pg_cursor.execute(f"ALTER TABLE raw.{pg_table} ADD COLUMN {c_name} {pg_type} NULL")
                    logging.info(f"  [{table_name}] Added missing column: {c_name}")

            # Ensure Primary Key exists for UPSERT logic
            if pk_cols:
                pg_cursor.execute("""
                    SELECT count(*)
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'raw'
                    AND table_name = %s
                    AND constraint_type = 'PRIMARY KEY'
                """, (pg_table,))
                if pg_cursor.fetchone()[0] == 0:
                    try:
                        pg_cursor.execute(f"ALTER TABLE raw.{pg_table} ADD PRIMARY KEY ({', '.join(pk_cols)})")
                        logging.info(f"  [{table_name}] Added missing Primary Key constraint")
                    except Exception as pk_err:
                        logging.warning(f"  [{table_name}] Could not add PK: {pk_err}. Using non-upsert fallback.")
                        pk_cols = []

        # 3. Handle Watermark and Incremental extraction
        last_watermark = get_watermark(pg_cursor, table_name)
        extraction_query = f"SELECT {', '.join(col_names)} FROM {ORACLE_SCHEMA}.{table_name}"
        params = {}
        
        if inc_col and last_watermark:
            col_meta = next((c for c in columns_meta if c[0].lower() == inc_col.lower()), None)
            is_date = col_meta and ('DATE' in col_meta[1].upper() or 'TIMESTAMP' in col_meta[1].upper())
            
            if is_date:
                extraction_query += f" WHERE {inc_col} > TO_TIMESTAMP(:wm, 'YYYY-MM-DD HH24:MI:SS.FF')"
            else:
                extraction_query += f" WHERE {inc_col} > :wm"
            params['wm'] = last_watermark
            decision = f"INCREMENTAL ({inc_col} > {last_watermark})"
        else:
            decision = "FULL LOAD"

        result['decision'] = decision
        logging.info(f"{table_name:25} | Decision: {decision}")

        # 4. Prepare UPSERT logic
        now = datetime.now()
        insert_cols = ', '.join(col_names) + ', etl_uploaded_date, etl_updated_date'
        
        if pk_cols:
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in col_names if col not in pk_cols])
            if not update_clause:
                update_clause = "etl_updated_date = EXCLUDED.etl_updated_date"
            else:
                update_clause += ", etl_updated_date = EXCLUDED.etl_updated_date"
                
            insert_sql = f"""
                INSERT INTO raw.{pg_table} ({insert_cols}) 
                VALUES %s 
                ON CONFLICT ({', '.join(pk_cols)}) 
                DO UPDATE SET {update_clause}
            """
        else:
            if not last_watermark:
                pg_cursor.execute(f"TRUNCATE TABLE raw.{pg_table}")
            insert_sql = f"INSERT INTO raw.{pg_table} ({insert_cols}) VALUES %s"

        # 5. Execute Migration
        ora_cursor.execute(extraction_query, **params)
        
        inserted = 0
        new_max_watermark = None
        col_indices = {name: i for i, name in enumerate(col_names)}
        inc_idx = col_indices.get(inc_col.lower()) if inc_col else None

        while True:
            rows = ora_cursor.fetchmany(BATCH_SIZE)
            if not rows: break
            
            if inc_idx is not None:
                batch_max = max(r[inc_idx] for r in rows if r[inc_idx] is not None)
                if new_max_watermark is None or (batch_max is not None and batch_max > new_max_watermark):
                    new_max_watermark = batch_max
            
            rows_with_etl = [r + (now, now) for r in rows]
            extras.execute_values(pg_cursor, insert_sql, rows_with_etl)
            inserted += len(rows)
            
            if inserted % (BATCH_SIZE * 20) == 0:
                logging.info(f"  ... {inserted} rows processed so far")
            #logging.info(f"  [{table_name}] ... processed {inserted} rows")

        # 6. Finalize
        if inserted > 0 and inc_col and new_max_watermark:
            w_type = 'TIMESTAMP' if hasattr(new_max_watermark, 'strftime') else 'ID'
            update_watermark(pg_cursor, table_name, new_max_watermark, w_type)
        
        pg_conn.commit()
        result['status'] = 'SUCCESS'
        result['rows'] = inserted
        result['duration'] = round(time.time() - start_time, 2)
        logging.info(f"  [{table_name}] COMPLETED: {inserted} rows in {result['duration']}s")
        return result

    except Exception as e:
        pg_conn.rollback()
        logging.error(f"  [{table_name}] FAILED: {e}")
        result['status'] = 'FAILED'
        result['error'] = str(e)
        result['duration'] = round(time.time() - start_time, 2)
        return result
    finally:
        ora_cursor.close()
        pg_cursor.close()

if __name__ == '__main__':
    ora, pg = None, None
    try:
        ora = oracledb.connect(**ORACLE_CONFIG)
        pg = psycopg2.connect(PG_CONFIG)
        
        results = []
        for t in TABLES_TO_MIGRATE:
            res = migrate_table(ora, pg, t)
            results.append(res)
            
        # Summary Report
        logging.info("\n" + "="*80)
        logging.info("FINAL MIGRATION SUMMARY")
        logging.info("="*80)
        logging.info(f"{'Table':<25} | {'Status':<10} | {'Rows':>10} | {'Duration':>8}")
        logging.info("-" * 80)
        
        total_rows = 0
        success_count = 0
        for r in results:
            status = r['status']
            rows = r['rows']
            duration = f"{r['duration']}s"
            logging.info(f"{r['table']:<25} | {status:<10} | {rows:>10} | {duration:>8}")
            if status == 'SUCCESS':
                success_count += 1
                total_rows += rows
        
        logging.info("-" * 80)
        logging.info(f"Total Tables Processed: {len(results)}")
        logging.info(f"Success: {success_count} | Failed: {len(results) - success_count}")
        logging.info(f"Total Rows Migrated: {total_rows}")
        logging.info("="*80)

    except Exception as e:
        logging.error(f"Critical Migration error: {e}")
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
