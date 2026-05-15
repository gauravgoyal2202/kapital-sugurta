import logging
import sys
import os

# Add utils directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

from etl_utils import get_pg_conn, get_ora_conn, update_metadata, init_metadata_table, send_email, setup_logging

# --- CONFIGURATION: List all tables that need a FULL refresh ---
FULL_REFRESH_CONFIG = [
    {"pg_table": "ins_vertical_oracle", "ora_table": "INS_VERTICAL"},
    # {"pg_table": "sp_division_oracle", "ora_table": "SP_DIVISION"},
    # {"pg_table": "ins_pgroup1_oracle", "ora_table": "INS_PGROUP1"},
    # {"pg_table": "ins_fifty_oracle", "ora_table": "INS_FIFTY"},
    # {"pg_table": "p_sp_currency_oracle", "ora_table": "P_SP_CURRENCY"},
    # {"pg_table": "ins_kurs_oracle", "ora_table": "INS_KURS"}
]

def run_full_refresh(pg_table, ora_table):
    setup_logging('etl_refresh.log')
    logging.info(f">>> Starting FULL REFRESH: {pg_table}")
    try:
        with get_ora_conn() as ora_conn, get_pg_conn() as pg_conn:
            with ora_conn.cursor() as ora_cur, pg_conn.cursor() as pg_cur:
                # 1. Pull data from Oracle
                ora_cur.execute(f"SELECT * FROM {ora_table}")
                rows = ora_cur.fetchall()
                colnames = [d[0] for d in ora_cur.description]
                
                # 2. SHADOW LOADING: Load into a temp table first
                temp_table = f"{pg_table}_temp"
                pg_cur.execute(f"DROP TABLE IF EXISTS raw.{temp_table}")
                pg_cur.execute(f"CREATE TABLE raw.{temp_table} (LIKE raw.{pg_table} INCLUDING ALL)")
                
                placeholders = ",".join(["%s"] * len(colnames))
                insert_sql = f"INSERT INTO raw.{temp_table} ({','.join(colnames)}) VALUES ({placeholders})"
                pg_cur.executemany(insert_sql, rows)
                
                # 3. TRANSACTIONAL SWAP: Truncate and Move (Happens in ms)
                pg_cur.execute(f"TRUNCATE TABLE raw.{pg_table}")
                pg_cur.execute(f"INSERT INTO raw.{pg_table} SELECT * FROM raw.{temp_table}")
                pg_cur.execute(f"DROP TABLE raw.{temp_table}")
                
                update_metadata(pg_table, "SUCCESS")
                logging.info(f"Successfully loaded {len(rows)} rows into {pg_table}")
            pg_conn.commit()
    except Exception as e:
        error_msg = f"Error refreshing {pg_table}: {str(e)}"
        logging.error(error_msg)
        update_metadata(pg_table, "FAILED")
        send_email(f"FAILURE: Full Refresh {pg_table}", error_msg)

if __name__ == "__main__":
    init_metadata_table()
    for config in FULL_REFRESH_CONFIG:
        run_full_refresh(config["pg_table"], config["ora_table"])
