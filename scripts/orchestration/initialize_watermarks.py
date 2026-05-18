import psycopg2

conn = psycopg2.connect(
    host="10.10.3.124",
    port=5432,
    dbname="kapital_insurance_dm",
    user="etl_user",
    password="kapital@123!"
)
cur = conn.cursor()

# List of incremental tables and their Postgres columns
tables = [
    {"pg_table": "ins_viplati_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_polis_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_anketa_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_bank_client_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_kontragent_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_loss_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_oplata_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_rastorg_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_regress_bank_oracle", "wm_col": "modified_date"},
    {"pg_table": "ins_regress_oracle", "wm_col": "ins_id"},
    {"pg_table": "ins_sobitie_oracle", "wm_col": "modified_date"},
    {"pg_table": "tb_anketa_oracle", "wm_col": "tb_id"},
    {"pg_table": "tb_avto_oracle", "wm_col": "tb_id"},
    {"pg_table": "tb_oplata_oracle", "wm_col": "tb_id"},
    {"pg_table": "tb_polis_oracle", "wm_col": "tb_datepl"}
]

print("Initializing watermarks from existing Postgres data...")

for t in tables:
    pg_table = t["pg_table"]
    wm_col = t["wm_col"]
    
    # 1. Get the max date/ID currently in Postgres
    cur.execute(f"SELECT MAX({wm_col}) FROM raw.{pg_table}")
    max_val = cur.fetchone()[0]
    
    if max_val is not None:
        # Format the value as string
        wm_str = str(max_val) if isinstance(max_val, (int, float)) else max_val.strftime('%Y-%m-%d %H:%M:%S')
        
        # 2. Insert into metadata table
        cur.execute("""
            INSERT INTO raw.etl_refresh_metadata (table_name, status, last_watermark_value, last_refresh_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE SET 
                last_watermark_value = EXCLUDED.last_watermark_value,
                status = 'SUCCESS',
                last_refresh_at = CURRENT_TIMESTAMP;
        """, (pg_table, 'SUCCESS', wm_str))
        
        print(f" -> {pg_table}: Watermark set to {wm_str}")
    else:
        print(f" -> {pg_table}: No data found, leaving empty.")

conn.commit()
cur.close()
conn.close()
print("All watermarks successfully initialized!")