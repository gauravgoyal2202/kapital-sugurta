import logging
import sys
import os

# Add utils directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

from etl_utils import get_pg_conn, get_ora_conn, update_metadata, init_metadata_table, send_email, setup_logging

# --- CONFIGURATION: Define Incremental Logic for Large Tables ---
INCREMENTAL_CONFIG = [
    {
        "pg_table": "ins_viplati_oracle", 
        "ora_table": "INS_VIPLATI", 
        "watermark_col": "MODIFIED_DATE", 
        "pk_col": "ins_id"
    },
    # {
    #     "pg_table": "ins_polis_oracle", 
    #     "ora_table": "INS_POLIS", 
    #     "watermark_col": "MODIFIED_DATE", 
    #     "pk_col": "sogl_id"
    # },
    # {
    #     "pg_table": "ins_anketa_oracle", 
    #     "ora_table": "INS_ANKETA", 
    #     "watermark_col": "MODIFIED_DATE", 
    #     "pk_col": "ins_id"
    # },
    # {
    #     "pg_table": "tb_polis_oracle", 
    #     "ora_table": "TB_POLIS", 
    #     "watermark_col": "TB_ID",   # Using ID as watermark for TB tables
    #     "pk_col": "tb_id"
    # }
]

# --- GLOBAL SETTINGS ---
LOOKBACK_DAYS = 30  # To handle late-arriving data and updates to old records

def run_incremental_refresh(pg_table, ora_table, watermark_col, pk_col):
    setup_logging('etl_refresh.log')
    logging.info(f">>> Starting INCREMENTAL REFRESH: {pg_table}")
    try:
        with get_pg_conn() as pg_conn:
            with pg_conn.cursor() as pg_cur:
                # 1. Fetch the last watermark
                pg_cur.execute("SELECT last_watermark_value FROM raw.etl_refresh_metadata WHERE table_name = %s", (pg_table,))
                res = pg_cur.fetchone()
                last_wm = res[0] if res else '1900-01-01 00:00:00'

                with get_ora_conn() as ora_conn:
                    with ora_conn.cursor() as ora_cur:
                        # 2. Extract with LOOK-BACK (to catch updates)
                        if last_wm.isdigit():
                            # For IDs, lookback isn't needed unless IDs can be re-used (rare)
                            query = f"SELECT * FROM {ora_table} WHERE {watermark_col} > {last_wm}"
                        else:
                            # For Dates, look back 30 days from the last watermark
                            query = f"SELECT * FROM {ora_table} WHERE {watermark_col} > TO_TIMESTAMP('{last_wm}', 'YYYY-MM-DD HH24:MI:SS') - {LOOKBACK_DAYS}"
                        
                        ora_cur.execute(query)
                        rows = ora_cur.fetchall()
                        
                        if not rows:
                            logging.info(f"No new/updated data for {pg_table}")
                            return

                        colnames = [d[0] for d in ora_cur.description]
                        wm_idx = colnames.index(watermark_col.upper())
                        
                        raw_max_wm = max(r[wm_idx] for r in rows)
                        new_wm = str(raw_max_wm) if isinstance(raw_max_wm, (int, float)) else raw_max_wm.strftime('%Y-%m-%d %H:%M:%S')

                        # 3. Use a temporary staging table to calculate New vs Updated
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

                        # 4. Final UPSERT from staging
                        updates = ",".join([f"{c} = EXCLUDED.{c}" for c in colnames if c.lower() != pk_col.lower()])
                        upsert_sql = f"""
                            INSERT INTO raw.{pg_table} ({cols_str}) 
                            SELECT {cols_str} FROM {temp_staging}
                            ON CONFLICT ({pk_col}) DO UPDATE SET {updates};
                        """
                        pg_cur.execute(upsert_sql)
                        
                        update_metadata(pg_table, "SUCCESS", new_wm)
                        logging.info(f"DONE: {pg_table} -> {new_count} New Records, {update_count} Updated/Re-checked Records.")
            pg_conn.commit()
    except Exception as e:
        error_msg = f"Error in incremental load for {pg_table}: {str(e)}"
        logging.error(error_msg)
        update_metadata(pg_table, "FAILED")
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
