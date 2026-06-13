import os
import sys
import logging
import oracledb
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

# Configure Logging
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'extract_reinsurance_oracle.log')
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('Reinsurance_Oracle_Extractor')

# Configuration
ORACLE_CONFIG = {
    'user': os.getenv('ORACLE_USER'),
    'password': os.getenv('ORACLE_PASS'),
    'dsn': f"{os.getenv('ORACLE_HOST')}:{os.getenv('ORACLE_PORT', 1521)}/{os.getenv('ORACLE_SERVICE')}"
}
PG_CONFIG = (
    f"host={os.getenv('PG_HOST')} "
    f"port={os.getenv('PG_PORT', '5432')} "
    f"dbname={os.getenv('PG_DATABASE')} "
    f"user={os.getenv('PG_USER')} "
    f"password={os.getenv('PG_PASSWORD')}"
)

# The New Oracle Query provided by the client
REINSURANCE_QUERY = """
Select
/* slip level */
r.id as reinsurance_id,
r.id as dog_id,
r.division_id,
KAPITALDB.dec_division(r.division_id, 1) as division_name,
r.direction,
decode(r.direction, 1, 'исходящий', 2, 'входящий иностранный', 'входящий') as direction_name,
r.contract_number as slip_contract_number,
r.contract_issue_date as slip_contract_issue_date,
r.reinsurance_start_date,
r.reinsurance_end_date,
r.reinsurer_is_foreign,
decode(r.reinsurer_is_foreign, 0, 'Местный', 1, 'Иностранный', ' ') as reinsurer_is_foreign_name,
r.reinsurer_org_id,
r.reinsurer_foreign_org_id,
r.reinsurant_ins_org_id,
r.reinsurant_foreign_ins_org_id,
KAPITALDB.get_reinsurer(r.id) as slip_reinsurer,
KAPITALDB.get_reinsurant(r.id) as slip_reinsurant,
r.obligator_condition,
r.obligator_other_condition,
r.slip_number,
r.slip_date,
r.reinsurance_form_id,
f.name as reinsurance_form_name,
r.reinsurance_type_id,
t.name as reinsurance_type_name,
r.broker_id,
b.name as broker_name,
r.broker_commission,
r.covernote_number,
r.covernote_date,
r.currency_id as slip_currency_id,
KAPITALDB.f_old_currency(r.currency_id) as slip_currency_name,
r.exchange_rate,
r.reinsurant_share,
r.reinsurant_share_in_forg_cur,
r.reinsurant_share_in_percents,
r.reinsurer_limit,
r.reinsurer_limit_in_foreign_cur,
r.reinsurer_limit_in_percents,
r.brutto_ins_premium,
r.brutto_ins_premium_in_forg_cur,
r.brutto_ins_premium_in_percents,
r.commission,
r.commission_in_foreign_currency,
r.commission_in_percents,
r.netto_ins_premium,
r.netto_ins_premium_in_forg_cur,
r.netto_ins_premium_in_percents,
r.netto_accrual_premium,
r.netto_accrual_date,
r.netto_accrual_commission,
r.brutto_accrual_premium,
r.deadline_for_full_prem_payment,
r.paid_premium,
r.premium_paid_date,
r.payment_type_id,
r.first_payment_deadline,
r.first_payment_sum,
r.first_payment_date,
r.first_payment_paid_sum,
r.user_id,
KAPITALDB.dec_user2(r.user_id) as created_user,
r.created_date,
r.mod_userid,
r.mod_date,
r.status as slip_status,
case when r.status < 0 then -1 else r.status end as iconstat,
r.fond_status,
decode(r.fond_status, 0, 'Не отправлен', 'Отправлен') as fond_status_name,
r.fond_uuid,
'0' as icondop,
/* related contract level */
c.id as contract_id,
c.id as plink,
c.is_foreign as contract_is_foreign,
decode(c.is_foreign, 0, 'местный', 'иностранный') as contract_is_foreign_name,
c.insurance_sum,
c.contract_number as contract_number,
c.contract_issue_date as contract_issue_date,
c.insurance_form_id as contract_insurance_form_id,
c.insurance_product_name,
c.insurant_name,
c.classes,
decode(
c.is_foreign,
0, KAPITALDB.get_reinsurer_org_name(c.insurance_org_id),
KAPITALDB.get_reinsurer_org_name(c.foreign_insurance_org_id, 1)
) as contract_reinsurer,
c.insurance_org_id,
c.foreign_insurance_org_id,
c.start_date as contract_start_date,
c.end_date as contract_end_date,
c.currency_id as contract_currency_id,
KAPITALDB.f_old_currency(c.currency_id) as contract_currency_name,
c.exchange_rate as contract_exchange_rate
from ins_reinsurance r
left join sp_reinsurance_form f
on r.reinsurance_form_id = f.id
left join sp_reinsurance_type t
on r.reinsurance_type_id = t.id
left join sp_reinsurance_brokers b
on r.broker_id = b.id
left join ins_reins_contract c
on c.reinsurance_id = r.id
where :direction = 0
or :direction = r.direction
"""

RAW_TABLE_NAME = "oracle_reinsurance_combined"

def map_oracle_to_pg_type(ora_type):
    # Simplified mapping for this specific result set
    if ora_type == oracledb.DB_TYPE_NUMBER: return 'NUMERIC'
    if ora_type in (oracledb.DB_TYPE_DATE, oracledb.DB_TYPE_TIMESTAMP): return 'TIMESTAMP'
    return 'TEXT'

def main():
    logger.info("Starting Reinsurance Oracle extraction...")
    
    ora_conn, pg_conn = None, None
    try:
        ora_conn = oracledb.connect(**ORACLE_CONFIG)
        pg_conn = psycopg2.connect(PG_CONFIG)
        
        ora_cur = ora_conn.cursor()
        pg_cur = pg_conn.cursor()
        
        # 1. Execute Query to get metadata
        # Setting direction to 0 to fetch all
        ora_cur.execute(REINSURANCE_QUERY, direction=0)
        
        columns = [col[0].lower() for col in ora_cur.description]
        col_types = [map_oracle_to_pg_type(col[1]) for col in ora_cur.description]
        
        # 2. Prepare PostgreSQL Table
        pg_col_defs = [f"{name} {dtype}" for name, dtype in zip(columns, col_types)]
        pg_col_defs.append("etl_loaded_at TIMESTAMP DEFAULT NOW()")
        
        pg_cur.execute(f"CREATE SCHEMA IF NOT EXISTS raw;")
        pg_cur.execute(f"DROP TABLE IF EXISTS raw.{RAW_TABLE_NAME};")
        pg_cur.execute(f"CREATE TABLE raw.{RAW_TABLE_NAME} ({', '.join(pg_col_defs)});")
        
        # 3. Batch Insert
        insert_sql = f"INSERT INTO raw.{RAW_TABLE_NAME} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        
        inserted_count = 0
        while True:
            rows = ora_cur.fetchmany(1000)
            if not rows:
                break
            extras.execute_values(pg_cur, insert_sql, rows)
            inserted_count += len(rows)
            logger.info(f"  Inserted {inserted_count} rows...")
            
        pg_conn.commit()
        logger.info(f"Extraction finished. Total rows: {inserted_count}")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        if pg_conn: pg_conn.rollback()
    finally:
        if ora_conn: ora_conn.close()
        if pg_conn: pg_conn.close()

if __name__ == '__main__':
    main()
