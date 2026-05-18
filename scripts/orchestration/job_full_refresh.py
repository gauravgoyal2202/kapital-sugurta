import logging
import sys
import os

# Add utils directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

from etl_utils import get_pg_conn, get_ora_conn, start_metadata_log, end_metadata_log, init_metadata_table, send_email, setup_logging

# --- CONFIGURATION: Complete list of all Reference/Metadata tables for FULL refresh ---
FULL_REFRESH_CONFIG = [
    {"pg_table": "ins_vertical_oracle", "ora_table": "INS_VERTICAL"},
    {"pg_table": "sp_division_oracle", "ora_table": "SP_DIVISION"},
    {"pg_table": "ins_pgroup1_oracle", "ora_table": "INS_PGROUP1"},
    {"pg_table": "ins_fifty_oracle", "ora_table": "INS_FIFTY"},
    {"pg_table": "p_sp_currency_oracle", "ora_table": "P_SP_CURRENCY"},
    {"pg_table": "ins_kurs_oracle", "ora_table": "INS_KURS"},
    {"pg_table": "ins_agent_akt_oracle", "ora_table": "INS_AGENT_AKT"},
    {"pg_table": "ins_department_oracle", "ora_table": "INS_DEPARTMENT"},
    {"pg_table": "ins_employee_oracle", "ora_table": "INS_EMPLOYEE"},
    {"pg_table": "ins_groups_oracle", "ora_table": "INS_GROUPS"},
    {"pg_table": "ins_headbanks_oracle", "ora_table": "INS_HEADBANKS"},
    {"pg_table": "ins_invdep_oracle", "ora_table": "INS_INVDEP"},
    {"pg_table": "ins_invloan_accrual_oracle", "ora_table": "INS_INVLOAN_ACCRUAL"},
    {"pg_table": "ins_invloan_oplata_oracle", "ora_table": "INS_INVLOAN_OPLATA"},
    {"pg_table": "ins_invloan_oracle", "ora_table": "INS_INVLOAN"},
    {"pg_table": "ins_pgroup2_oracle", "ora_table": "INS_PGROUP2"},
    {"pg_table": "ins_pgroup3_oracle", "ora_table": "INS_PGROUP3"},
    {"pg_table": "ins_pturi_oracle", "ora_table": "INS_PTURI"},
    {"pg_table": "ins_reins_contract_oracle", "ora_table": "INS_REINS_CONTRACT"},
    {"pg_table": "ins_reinsurance_oracle", "ora_table": "INS_REINSURANCE"},
    {"pg_table": "sp_country_oracle", "ora_table": "SP_COUNTRY"},
    {"pg_table": "sp_orgtype_oracle", "ora_table": "SP_ORGTYPE"},
    {"pg_table": "sp_reinsurance_brokers_oracle", "ora_table": "SP_REINSURANCE_BROKERS"},
    {"pg_table": "sp_reinsurance_foreign_org_oracle", "ora_table": "SP_REINSURANCE_FOREIGN_ORG"},
    {"pg_table": "sp_reinsurance_form_oracle", "ora_table": "SP_REINSURANCE_FORM"},
    {"pg_table": "sp_reinsurance_org_oracle", "ora_table": "SP_REINSURANCE_ORG"},
    {"pg_table": "sp_reinsurance_type_oracle", "ora_table": "SP_REINSURANCE_TYPE"},
    {"pg_table": "tb_users_oracle", "ora_table": "TB_USERS"}
]

def run_full_refresh(pg_table, ora_table):
    setup_logging('etl_refresh.log')
    logging.info(f">>> Starting FULL REFRESH: {pg_table}")
    
    # 1. Start execution audit log (Status: RUNNING)
    run_id, _ = start_metadata_log(
        target_table=pg_table, 
        refresh_type="FULL", 
        source_table=ora_table, 
        load_strategy="TRUNCATE_LOAD"
    )
    
    try:
        with get_ora_conn() as ora_conn, get_pg_conn() as pg_conn:
            with ora_conn.cursor() as ora_cur, pg_conn.cursor() as pg_cur:
                # 2. Pull data from Oracle
                ora_cur.execute(f"SELECT * FROM {ora_table}")
                rows = ora_cur.fetchall()
                colnames = [d[0] for d in ora_cur.description]
                
                # 3. SHADOW LOADING: Load into a temp table first
                temp_table = f"{pg_table}_temp"
                pg_cur.execute(f"DROP TABLE IF EXISTS raw.{temp_table}")
                pg_cur.execute(f"CREATE TABLE raw.{temp_table} (LIKE raw.{pg_table} INCLUDING ALL)")
                
                placeholders = ",".join(["%s"] * len(colnames))
                insert_sql = f"INSERT INTO raw.{temp_table} ({','.join(colnames)}) VALUES ({placeholders})"
                pg_cur.executemany(insert_sql, rows)
                
                # 4. TRANSACTIONAL SWAP: Truncate and Move (Happens in ms)
                pg_cur.execute(f"TRUNCATE TABLE raw.{pg_table}")
                pg_cur.execute(f"INSERT INTO raw.{pg_table} SELECT * FROM raw.{temp_table}")
                pg_cur.execute(f"DROP TABLE raw.{temp_table}")
                
                # 5. Log Success (Status: SUCCESS)
                end_metadata_log(
                    run_id=run_id,
                    status="SUCCESS",
                    rows_extracted=len(rows),
                    rows_inserted=len(rows),
                    rows_updated=0
                )
                logging.info(f"Successfully loaded {len(rows)} rows into {pg_table}")
            pg_conn.commit()
    except Exception as e:
        error_msg = f"Error refreshing {pg_table}: {str(e)}"
        logging.error(error_msg)
        
        # 5. Log Failure (Status: FAILED)
        end_metadata_log(
            run_id=run_id,
            status="FAILED",
            rows_extracted=0,
            rows_inserted=0,
            rows_updated=0,
            error_message=error_msg
        )
        send_email(f"FAILURE: Full Refresh {pg_table}", error_msg)

if __name__ == "__main__":
    init_metadata_table()
    for config in FULL_REFRESH_CONFIG:
        run_full_refresh(config["pg_table"], config["ora_table"])
