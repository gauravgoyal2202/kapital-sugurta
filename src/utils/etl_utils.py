import os
import psycopg2
import oracledb
import logging
from dotenv import load_dotenv

# Automatically fetch CLOBs as strings and BLOBs as bytes to prevent 'can't adapt type LOB' errors in Postgres
oracledb.defaults.fetch_lobs = False

# Load configuration
# This looks for .env in the parent directory of 'utils', which is the project root
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
load_dotenv(dotenv_path)

def setup_logging(log_filename='etl_process.log'):
    """Configures logging to a specific file in the logs folder."""
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_filename)

    # Reset handlers if they already exist
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- SMTP CONFIGURATION (Pulled from .env) ---
SMTP_CONFIG = {
    "server": os.getenv("ALERT_SMTP_HOST"),
    "port": int(os.getenv("ALERT_SMTP_PORT", 587)),
    "user": os.getenv("ALERT_SMTP_USER"),
    "pass": os.getenv("ALERT_SMTP_PASS")
}
SMTP_TO = os.getenv("ALERT_TO", "responsible_person@kapital.uz")

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )

def get_ora_conn():
    return oracledb.connect(
        user=os.getenv("ORA_USER", "ORACLE_USER_HERE"),
        password=os.getenv("ORA_PASS", "ORACLE_PASS_HERE"),
        dsn=os.getenv("ORA_DSN", "ORACLE_DSN_HERE")
    )

def init_metadata_table():
    """Ensures the enterprise audit table exists in Postgres with auto-migration from the old schema."""
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            # 1. Check if the table exists and if it is the old schema
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'raw' AND table_name = 'etl_refresh_metadata'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            is_old_schema = False
            if table_exists:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = 'raw' AND table_name = 'etl_refresh_metadata' AND column_name = 'run_id'
                    );
                """)
                is_old_schema = not cur.fetchone()[0]
            
            if is_old_schema:
                logging.info("Old metadata schema detected. Migrating watermarks safely...")
                # Rename the old table
                cur.execute("ALTER TABLE raw.etl_refresh_metadata RENAME TO etl_refresh_metadata_old;")
                
            # 2. Create the brand new enterprise schema
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.etl_refresh_metadata (
                    run_id BIGSERIAL PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    source_system TEXT NOT NULL,
                    source_schema TEXT,
                    source_table_name TEXT,
                    target_system TEXT DEFAULT 'postgres',
                    target_schema TEXT NOT NULL,
                    target_table_name TEXT NOT NULL,
                    refresh_type TEXT NOT NULL,
                    load_strategy TEXT,
                    watermark_column TEXT,
                    last_watermark_value TEXT,
                    new_watermark_value TEXT,
                    primary_key_columns TEXT,
                    incremental_condition TEXT,
                    rows_extracted BIGINT DEFAULT 0,
                    rows_inserted BIGINT DEFAULT 0,
                    rows_updated BIGINT DEFAULT 0,
                    rows_deleted BIGINT DEFAULT 0,
                    rows_rejected BIGINT DEFAULT 0,
                    source_row_count BIGINT,
                    target_row_count BIGINT,
                    data_size_mb NUMERIC(18,2),
                    refresh_start_time TIMESTAMP NOT NULL,
                    refresh_end_time TIMESTAMP,
                    duration_seconds NUMERIC(18,2),
                    status TEXT NOT NULL,
                    error_message TEXT,
                    error_code TEXT,
                    retry_count INT DEFAULT 0,
                    batch_id TEXT,
                    execution_order INT,
                    source_file_name TEXT,
                    source_file_path TEXT,
                    api_endpoint TEXT,
                    checksum_value TEXT,
                    validation_status TEXT,
                    validation_message TEXT,
                    created_by TEXT DEFAULT CURRENT_USER,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # 3. If we renamed the old table, copy watermarks and drop it
            if is_old_schema:
                cur.execute("""
                    INSERT INTO raw.etl_refresh_metadata (
                        pipeline_name, source_system, target_schema, target_table_name,
                        refresh_type, last_watermark_value, new_watermark_value,
                        refresh_start_time, refresh_end_time, status
                    )
                    SELECT 
                        'oracle_to_postgres', 'oracle', 'raw', table_name,
                        'INCREMENTAL', last_watermark_value, last_watermark_value,
                        last_refresh_at, last_refresh_at, status
                    FROM raw.etl_refresh_metadata_old;
                """)
                cur.execute("DROP TABLE raw.etl_refresh_metadata_old;")
                logging.info("Safe migration completed successfully! All watermarks preserved.")
                
            # 4. Create proper indexes for high-performance Power BI dashboard querying
            cur.execute("CREATE INDEX IF NOT EXISTS idx_etl_metadata_target_table ON raw.etl_refresh_metadata (target_table_name);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_etl_metadata_batch_id ON raw.etl_refresh_metadata (batch_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_etl_metadata_start_time ON raw.etl_refresh_metadata (refresh_start_time DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_etl_metadata_status ON raw.etl_refresh_metadata (status);")
            
        conn.commit()

def start_metadata_log(target_table, refresh_type, source_table=None, watermark_col=None, primary_keys=None, load_strategy=None):
    """Inserts a new run record with 'RUNNING' status and returns the run_id."""
    import os
    import datetime
    
    batch_id = os.getenv("ETL_BATCH_ID") or datetime.datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")
    start_time = datetime.datetime.now()
    
    # Try to find the last successful watermark value for this table
    last_wm = None
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT new_watermark_value 
                FROM raw.etl_refresh_metadata 
                WHERE target_table_name = %s AND status = 'SUCCESS' 
                ORDER BY refresh_end_time DESC LIMIT 1;
            """, (target_table,))
            res = cur.fetchone()
            if res:
                last_wm = res[0]
                
    # Insert new RUNNING log
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw.etl_refresh_metadata (
                    pipeline_name, source_system, source_schema, source_table_name,
                    target_schema, target_table_name, refresh_type, load_strategy,
                    watermark_column, last_watermark_value, primary_key_columns,
                    refresh_start_time, status, batch_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING run_id;
            """, (
                'oracle_to_postgres', 'oracle', 'KAPITAL_SUGURTA', source_table or target_table.replace('_oracle', '').upper(),
                'raw', target_table, refresh_type, load_strategy,
                watermark_col, last_wm, primary_keys,
                start_time, 'RUNNING', batch_id
            ))
            run_id = cur.fetchone()[0]
        conn.commit()
    return run_id, last_wm

def end_metadata_log(run_id, status, rows_extracted=0, rows_inserted=0, rows_updated=0, new_watermark=None, error_message=None):
    """Updates the existing run log to set success or failure status and statistics."""
    import datetime
    
    end_time = datetime.datetime.now()
    
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            # 1. Fetch start time
            cur.execute("SELECT refresh_start_time FROM raw.etl_refresh_metadata WHERE run_id = %s", (run_id,))
            res = cur.fetchone()
            start_time = res[0] if res else end_time
            
            duration = (end_time - start_time).total_seconds()
            
            # 2. Update stats and status
            cur.execute("""
                UPDATE raw.etl_refresh_metadata SET 
                    status = %s,
                    refresh_end_time = %s,
                    duration_seconds = %s,
                    rows_extracted = %s,
                    rows_inserted = %s,
                    rows_updated = %s,
                    new_watermark_value = %s,
                    error_message = %s,
                    updated_at = NOW()
                WHERE run_id = %s;
            """, (status, end_time, duration, rows_extracted, rows_inserted, rows_updated, new_watermark, error_message, run_id))
        conn.commit()

# Retain old signature as a wrapper for backward-compatibility if needed
def update_metadata(table_name, status, watermark=None, refresh_type=None, start_time=None, end_time=None, rows_processed=None, rows_inserted=None, rows_updated=None):
    """Legacy wrapper for update_metadata mapping directly to the new database table."""
    import datetime
    st = start_time or datetime.datetime.now()
    et = end_time or datetime.datetime.now()
    
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw.etl_refresh_metadata (
                    pipeline_name, source_system, target_schema, target_table_name,
                    refresh_type, last_watermark_value, new_watermark_value,
                    refresh_start_time, refresh_end_time, duration_seconds,
                    rows_extracted, rows_inserted, rows_updated, status
                )
                VALUES ('oracle_to_postgres', 'oracle', 'raw', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                table_name, refresh_type or 'FULL', watermark, watermark,
                st, et, (et - st).total_seconds(),
                rows_processed or 0, rows_inserted or 0, rows_updated or 0, status
            ))
        conn.commit()

def send_email(subject, body, to_email=None):
    """Standard utility to send email alerts with a fallback to logging."""
    target_email = to_email or SMTP_TO
    
    # Check if SMTP is configured
    if not SMTP_CONFIG["server"] or not SMTP_CONFIG["user"]:
        logging.warning("!!! SMTP NOT CONFIGURED !!!")
        logging.warning(f"ALERT SUBJECT: {subject}")
        logging.warning(f"ALERT BODY: \n{body}")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_CONFIG["user"]
        msg['To'] = target_email
        msg['Subject'] = f"[ETL ALERT] {subject}"
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP(SMTP_CONFIG["server"], SMTP_CONFIG["port"])
        server.starttls()
        server.login(SMTP_CONFIG["user"], SMTP_CONFIG["pass"])
        server.send_message(msg)
        server.quit()
        logging.info(f"Email alert sent to {target_email}")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        logging.warning(f"ORIGINAL ALERT: {subject} - {body}")
