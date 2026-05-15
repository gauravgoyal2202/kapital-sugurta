import logging
import sys
import os

# Add utils directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

from etl_utils import get_pg_conn, send_email, setup_logging

def run_dq_checks():
    setup_logging('etl_dq_check.log')
    logging.info(">>> Starting DATA QUALITY (DQ) CHECKS")
    issues = []
    
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                
                # 1. Check for unexpected volume drops (Example: TB_POLIS)
                cur.execute("SELECT COUNT(*) FROM raw.tb_polis_oracle")
                current_count = cur.fetchone()[0]
                if current_count < 1000000: # Threshold example
                    issues.append(f"CRITICAL: tb_polis_oracle row count ({current_count}) is unusually low!")

                # 2. Check for NULLs in critical columns (Using tb_id for polis)
                cur.execute("SELECT COUNT(*) FROM raw.ins_polis_oracle WHERE tb_id IS NULL")
                null_ids = cur.fetchone()[0]
                if null_ids > 0:
                    issues.append(f"WARNING: Found {null_ids} records with NULL tb_id in ins_polis_oracle")

                # 3. Check for invalid future dates in claims
                cur.execute("SELECT COUNT(*) FROM raw.ins_viplati_oracle WHERE date_viplata > CURRENT_DATE + INTERVAL '1 day'")
                future_dates = cur.fetchone()[0]
                if future_dates > 10: # Allow small margin for timezone/entry issues
                    issues.append(f"WARNING: Found {future_dates} future-dated claims in ins_viplati_oracle")

        if issues:
            body = "\n".join(issues)
            logging.warning(f"DQ Issues found:\n{body}")
            send_email("DQ DISCREPANCY DETECTED", f"The following anomalies were found during the daily DQ run:\n\n{body}")
        else:
            logging.info("All DQ checks passed.")
            
    except Exception as e:
        logging.error(f"Error during DQ check: {str(e)}")
        send_email("FAILURE: DQ Check Job", str(e))

if __name__ == "__main__":
    run_dq_checks()
