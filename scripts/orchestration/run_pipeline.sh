#!/bin/bash

# --- CONFIGURATION (Industry Standard Paths) ---
PROJECT_ROOT="/home/kapital/kapital_bi_pipeline"
ETL_DIR="$PROJECT_ROOT/etl"
DBT_DIR="$PROJECT_ROOT/dbt"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/pipeline_$(date +%Y%m%d).log"

# Ensure log directory exists
mkdir -p $LOG_DIR

echo "------------------------------------------------" >> $LOG_FILE
echo "Pipeline Ingestion & Transformation started at: $(date)" >> $LOG_FILE

# 1. Full Refresh (Reference Data)
echo "[1/5] Running Full Refresh Jobs..." >> $LOG_FILE
python3 $ETL_DIR/jobs/job_full_refresh.py >> $LOG_FILE 2>&1

# 2. Incremental Refresh (Fact Data)
echo "[2/5] Running Incremental Refresh Jobs..." >> $LOG_FILE
python3 $ETL_DIR/jobs/job_incremental_refresh.py >> $LOG_FILE 2>&1

# 3. Data Quality Checks
echo "[3/5] Running Data Quality Validation..." >> $LOG_FILE
python3 $ETL_DIR/jobs/job_data_quality.py >> $LOG_FILE 2>&1

# 4. NPS Survey Load (Excel)
echo "[4/5] Loading NPS Survey Data..." >> $LOG_FILE
python3 $ETL_DIR/jobs/job_load_nps.py >> $LOG_FILE 2>&1

# 5. dbt Transformations
echo "[5/5] Running dbt Transformations..." >> $LOG_FILE
cd $DBT_DIR || exit
dbt run >> $LOG_FILE 2>&1

echo "Pipeline finished at: $(date)" >> $LOG_FILE
echo "------------------------------------------------" >> $LOG_FILE
