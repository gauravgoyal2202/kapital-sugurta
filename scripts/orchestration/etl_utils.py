import os
import psycopg2
import oracledb
import logging
from dotenv import load_dotenv

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
    "server": os.getenv("SMTP_SERVER"),
    "port": int(os.getenv("SMTP_PORT", 587)),
    "user": os.getenv("SMTP_USER"),
    "pass": os.getenv("SMTP_PASS")
}
SMTP_TO = os.getenv("SMTP_TO", "responsible_person@kapital.uz")

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
    """Ensures the tracking table exists in Postgres."""
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.etl_refresh_metadata (
                    table_name TEXT PRIMARY KEY,
                    last_refresh_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_refresh_at_utc TIMESTAMP DEFAULT (now() at time zone 'utc'),
                    last_watermark_value TEXT,
                    status TEXT
                );
            """)
        conn.commit()

def update_metadata(table_name, status, watermark=None):
    """Logs the result of a refresh task with UTC support."""
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw.etl_refresh_metadata (table_name, status, last_watermark_value, last_refresh_at, last_refresh_at_utc)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, (now() at time zone 'utc'))
                ON CONFLICT (table_name) DO UPDATE SET 
                    status = EXCLUDED.status, 
                    last_watermark_value = COALESCE(EXCLUDED.last_watermark_value, raw.etl_refresh_metadata.last_watermark_value),
                    last_refresh_at = CURRENT_TIMESTAMP,
                    last_refresh_at_utc = (now() at time zone 'utc');
            """, (table_name, status, watermark))
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
