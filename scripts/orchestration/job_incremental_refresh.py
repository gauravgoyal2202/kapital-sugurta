import logging
import sys
import os

# Add utils directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

from etl_utils import get_pg_conn, get_ora_conn, start_metadata_log, end_metadata_log, init_metadata_table, send_email, setup_logging

# --- CONFIGURATION: Complete list of all Transactional/Incremental tables ---
INCREMENTAL_CONFIG = [
    {
        "pg_table": "ins_viplati_oracle", 
        "ora_table": "INS_VIPLATI", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "ins_id"
    },
    {
        "pg_table": "ins_polis_oracle", 
        "ora_table": "INS_POLIS", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "tb_id"
    },
    {
        "pg_table": "ins_anketa_oracle", 
        "ora_table": "INS_ANKETA", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "ins_id"
    },
    {
        "pg_table": "ins_bank_client_oracle", 
        "ora_table": "INS_BANK_CLIENT", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "ins_id"
    },
    {
        "pg_table": "ins_kontragent_oracle", 
        "ora_table": "INS_KONTRAGENT", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "tb_id"
    },
    {
        "pg_table": "ins_loss_oracle", 
        "ora_table": "INS_LOSS", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "ins_id"
    },
    {
        "pg_table": "ins_oplata_oracle", 
        "ora_table": "INS_OPLATA", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "ins_id"
    },
    {
        "pg_table": "ins_rastorg_oracle", 
        "ora_table": "INS_RASTORG", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "tb_id"
    },
    {
        "pg_table": "ins_regress_bank_oracle", 
        "ora_table": "INS_REGRESS_BANK", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "ins_id"
    },
    {
        "pg_table": "ins_regress_oracle", 
        "ora_table": "INS_REGRESS", 
        "watermark_col": "ins_id", # No modified_date, fallback to incremental ID
        "pk_col": "ins_id"
    },
    {
        "pg_table": "ins_sobitie_oracle", 
        "ora_table": "INS_SOBITIE", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "ins_id"
    },
    {
        "pg_table": "tb_anketa_oracle", 
        "ora_table": "TB_ANKETA", 
        "watermark_col": "tb_id", # No modified_date, fallback to incremental ID
        "pk_col": "tb_id"
    },
    {
        "pg_table": "tb_avto_oracle", 
        "ora_table": "TB_AVTO", 
        "watermark_col": "tb_id", # No modified_date, fallback to incremental ID
        "pk_col": "tb_id"
    },
    {
        "pg_table": "tb_oplata_oracle", 
        "ora_table": "TB_OPLATA", 
        "watermark_col": "tb_id", # No modified_date, fallback to incremental ID
        "pk_col": "tb_id"
    },
    {
        "pg_table": "tb_polis_oracle", 
        "ora_table": "TB_POLIS", 
        "watermark_col": "tb_datepl", # Date watermark
        "pk_col": "tb_id"
    }
]

# --- GLOBAL SETTINGS ---
LOOKBACK_DAYS = 30  # To handle late-arriving data and updates to old records

def run_incremental_refresh(pg_table, ora_table, watermark_col, pk_col):
    setup_logging('etl_refresh.log')
    logging.info(f">>> Starting INCREMENTAL REFRESH: {pg_table}")
    
    # 1. Start execution audit log (Status: RUNNING) and fetch last watermark
    run_id, last_wm = start_metadata_log(
        target_table=pg_table, 
        refresh_type="INCREMENTAL", 
        source_table=ora_table, 
        watermark_col=watermark_col,
        primary_keys=pk_col,
        load_strategy="UPSERT"
    )
    
    # If no watermark was found in history, use our type-safe default
    if last_wm is None:
        last_wm = '0' if watermark_col.lower() in ['ins_id', 'tb_id'] else '1900-01-01 00:00:00'
        
    try:
        with get_pg_conn() as pg_conn:
            with pg_conn.cursor() as pg_cur:
                with get_ora_conn() as ora_conn:
                    with ora_conn.cursor() as ora_cur:
                        # 2. Extract with LOOK-BACK (using TO_DATE to support Oracle date subtraction safely)
                        if last_wm.isdigit():
                            query = f"SELECT * FROM {ora_table} WHERE {watermark_col} > {last_wm}"
                        else:
                            # Oracle FIX: Use TO_DATE instead of TO_TIMESTAMP to avoid ORA-00932 Expected Timestamp got Number during date subtraction
                            query = f"SELECT * FROM {ora_table} WHERE {watermark_col} > TO_DATE('{last_wm}', 'YYYY-MM-DD HH24:MI:SS') - {LOOKBACK_DAYS}"
                        
                        ora_cur.execute(query)
                        rows = ora_cur.fetchall()
                        
                        if not rows:
                            # 3. Log Success with 0 rows (Status: SUCCESS)
                            end_metadata_log(
                                run_id=run_id,
                                status="SUCCESS",
                                rows_extracted=0,
                                rows_inserted=0,
                                rows_updated=0,
                                new_watermark=last_wm
                            )
                            logging.info(f"No new/updated data for {pg_table}")
                            return

                        colnames = [d[0] for d in ora_cur.description]
                        wm_idx = colnames.index(watermark_col.upper())
                        
                        raw_max_wm = max(r[wm_idx] for r in rows)
                        new_wm = str(raw_max_wm) if isinstance(raw_max_wm, (int, float)) else raw_max_wm.strftime('%Y-%m-%d %H:%M:%S')

                        # 4. Use a temporary staging table to calculate New vs Updated
                        temp_staging = f"{pg_table}_staging"
                        pg_cur.execute(f"DROP TABLE IF EXISTS {temp_staging}")
                        pg_cur.execute(f"CREATE TEMP TABLE {temp_staging} (LIKE raw.{pg_table} INCLUDING ALL)")
                        
                        cols_str = ",".join(colnames)
                        placeholders = ",".join(["%s"] * len(colnames))
                        insert_staging_sql = f"INSERT INTO {temp_staging} ({cols_str}) VALUES ({placeholders})"
                        pg_cur.executemany(insert_staging_sql, rows)
                        
                        # Calculate counts
                        pg_cur.execute(f"""
                            SELECT 
                                COUNT(*) FILTER (WHERE main.{pk_col} IS NULL) as new_records,
                                COUNT(*) FILTER (WHERE main.{pk_col} IS NOT NULL) as updated_records
                            FROM {temp_staging} stg
                            LEFT JOIN raw.{pg_table} main ON stg.{pk_col} = main.{pk_col}
                        """)
                        new_count, update_count = pg_cur.fetchone()

                        # 5. Final UPSERT from staging
                        updates = ",".join([f"{c} = EXCLUDED.{c}" for c in colnames if c.lower() != pk_col.lower()])
                        upsert_sql = f"""
                            INSERT INTO raw.{pg_table} ({cols_str}) 
                            SELECT {cols_str} FROM {temp_staging}
                            ON CONFLICT ({pk_col}) DO UPDATE SET {updates};
                        """
                        pg_cur.execute(upsert_sql)
                        
                        # 6. Log Success (Status: SUCCESS)
                        end_metadata_log(
                            run_id=run_id,
                            status="SUCCESS",
                            rows_extracted=len(rows),
                            rows_inserted=new_count,
                            rows_updated=update_count,
                            new_watermark=new_wm
                        )
                        logging.info(f"DONE: {pg_table} -> {new_count} New Records, {update_count} Updated/Re-checked Records.")
            pg_conn.commit()
    except Exception as e:
        error_msg = f"Error in incremental load for {pg_table}: {str(e)}"
        logging.error(error_msg)
        
        # 6. Log Failure (Status: FAILED)
        end_metadata_log(
            run_id=run_id,
            status="FAILED",
            rows_extracted=0,
            rows_inserted=0,
            rows_updated=0,
            error_message=error_msg
        )
        send_email(f"FAILURE: Incremental Refresh {pg_table}", error_msg)

if __name__ == "__main__":
    init_metadata_table()
    for config in INCREMENTAL_CONFIG:
        run_incremental_refresh(
            config["pg_table"], 
            config["ora_table"], 
            config["watermark_col"], 
            config["pk_col"]
        )
