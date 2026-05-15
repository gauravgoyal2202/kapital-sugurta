import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import sys
import logging

# Add utils directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from etl_utils import get_pg_conn, setup_logging, send_email

def run_nps_load():
    setup_logging('etl_nps_load.log')
    logging.info(">>> Starting NPS Survey Data Load")
    
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'Survey_Responses_Kapital_Sugurta.xlsx'))
    
    if not os.path.exists(file_path):
        logging.error(f"Excel file not found at {file_path}")
        return

    try:
        # 1. Read Sheets
        sheets = {
            'Form Responses 1': 'General NPS',
            'Form Responses 2': 'General NPS',
            'Form Responses 3': 'Employee NPS'
        }
        
        dfs = []
        for sheet, survey_type in sheets.items():
            try:
                df = pd.read_excel(file_path, sheet_name=sheet)
                if len(df) > 0:
                    # Map columns by position since headers are long Cyrillic strings
                    # Col 0: Timestamp, Col 1: Score, Col 2: Comment
                    df = df.iloc[:, [0, 1, 2]]
                    df.columns = ['response_timestamp', 'nps_score_raw', 'comment_text']
                    df['survey_type'] = survey_type
                    df['source_sheet'] = sheet
                    dfs.append(df)
            except Exception as e:
                logging.warning(f"Could not read sheet {sheet}: {e}")

        if not dfs:
            logging.info("No data found in any sheet.")
            return

        combined = pd.concat(dfs, ignore_index=True)
        
        # 2. Prepare for DB
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                # Create Table if not exists, then TRUNCATE (to preserve view dependencies)
                cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS raw;
                    CREATE TABLE IF NOT EXISTS raw.nps_survey_responses (
                        id               SERIAL PRIMARY KEY,
                        response_timestamp TIMESTAMP,
                        nps_score_raw    NUMERIC(4,1),
                        comment_text     TEXT,
                        survey_type      VARCHAR(50),
                        source_sheet     VARCHAR(50),
                        etl_loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    TRUNCATE TABLE raw.nps_survey_responses;
                """)
                
                # Insert data
                rows = [
                    (
                        row['response_timestamp'].to_pydatetime() if pd.notna(row['response_timestamp']) else None,
                        float(row['nps_score_raw']) if pd.notna(row['nps_score_raw']) else None,
                        str(row['comment_text']) if pd.notna(row['comment_text']) else None,
                        row['survey_type'],
                        row['source_sheet']
                    )
                    for _, row in combined.iterrows()
                ]
                
                execute_values(cur, """
                    INSERT INTO raw.nps_survey_responses 
                        (response_timestamp, nps_score_raw, comment_text, survey_type, source_sheet)
                    VALUES %s
                """, rows)
                
            conn.commit()
            logging.info(f"Successfully loaded {len(rows)} NPS responses into raw.nps_survey_responses")

    except Exception as e:
        logging.error(f"Error during NPS load: {str(e)}")
        send_email("FAILURE: NPS Survey Load", str(e))

if __name__ == "__main__":
    run_nps_load()
