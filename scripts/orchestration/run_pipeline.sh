#!/bin/bash
# ==============================================================================
# Enterprise Data Pipeline Orchestration Script
# Chaines Ingestion (Full & Incremental) -> Transformation (DBT Run & Test)
# ==============================================================================

# 1. Pipeline Environment Configuration
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
PROJECT_ROOT="/home/kapital/kapital_bi_pipeline"
ETL_DIR="$PROJECT_ROOT/etl"
DBT_DIR="$PROJECT_ROOT/dbt/kapital_sugurta_dbt"
LOG_DIR="$PROJECT_ROOT/logs"

mkdir -p "$LOG_DIR"
MASTER_LOG="$LOG_DIR/daily_pipeline_orchestration.log"

# Generate a Unified Batch ID for telemetry tracking across all jobs
export ETL_BATCH_ID="BATCH_$(date -u +'%Y%m%d_%H%M%S')"

log_message() {
    echo "[$(date -u +'%Y-%m-%d %H:%M:%S UTC')] [BATCH: $ETL_BATCH_ID] $1" | tee -a "$MASTER_LOG"
}

log_message "=========================================="
log_message ">>> STARTING DAILY BI DATA PIPELINE RUN <<<"
log_message "=========================================="

# Navigate to project root
cd "$PROJECT_ROOT" || {
    log_message "CRITICAL: Could not navigate to project root: $PROJECT_ROOT. Aborting."
    exit 1
}

# Activate Python Virtual Environment
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    log_message "Python virtual environment activated successfully."
else
    log_message "CRITICAL: Virtual environment 'venv' not found at $PROJECT_ROOT/venv. Aborting."
    exit 1
fi

# Define Python executable (uses venv's python since it's activated)
PYTHON_EXEC="python3"

# 2. RUN INGESTION STAGE: Full Refreshes (Metadata & Reference tables)
log_message ">>> Starting STEP 1: Full Ingestion (Reference Tables)..."
$PYTHON_EXEC "$ETL_DIR/jobs/job_full_refresh.py" >> "$LOG_DIR/job_full_refresh.log" 2>&1
FULL_STATUS=$?

if [ $FULL_STATUS -eq 0 ]; then
    log_message "SUCCESS: Reference tables full refresh loaded."
else
    log_message "CRITICAL: Reference tables full refresh FAILED. Halting pipeline to protect integrity."
    exit 1
fi

# 3. RUN INGESTION STAGE: Incremental Refreshes (Transactional & Heavy tables)
log_message ">>> Starting STEP 2: Incremental Ingestion (Transactional Tables)..."
$PYTHON_EXEC "$ETL_DIR/jobs/job_incremental_refresh.py" >> "$LOG_DIR/job_incremental_refresh.log" 2>&1
INC_STATUS=$?

if [ $INC_STATUS -eq 0 ]; then
    log_message "SUCCESS: Transactional tables incremental refresh completed."
else
    log_message "CRITICAL: Transactional tables incremental refresh FAILED. Halting pipeline."
    exit 1
fi

# 4. RUN ADDITIONAL JOBS: Data Quality & NPS (If files exist)
if [ -f "$ETL_DIR/jobs/job_data_quality.py" ]; then
    log_message ">>> Starting STEP 3: Running Data Quality Validations..."
    $PYTHON_EXEC "$ETL_DIR/jobs/job_data_quality.py" >> "$LOG_DIR/job_data_quality.log" 2>&1
fi

if [ -f "$ETL_DIR/jobs/job_load_nps.py" ]; then
    log_message ">>> Starting STEP 4: Loading NPS Survey Data..."
    $PYTHON_EXEC "$ETL_DIR/jobs/job_load_nps.py" >> "$LOG_DIR/job_load_nps.log" 2>&1
fi

# 5. RUN TRANSFORMATION STAGE: DBT Models
log_message ">>> Starting STEP 5: DBT Transformations (Building Data Marts)..."
if [ -d "$DBT_DIR" ]; then
    cd "$DBT_DIR" || {
        log_message "CRITICAL: Could not navigate to DBT project folder at $DBT_DIR. Halting."
        exit 1
    }
    
    dbt run --profiles-dir . >> "$LOG_DIR/dbt_run.log" 2>&1
    DBT_RUN_STATUS=$?

    if [ $DBT_RUN_STATUS -eq 0 ]; then
        log_message "SUCCESS: DBT models successfully generated."
    else
        log_message "CRITICAL: DBT models generation FAILED. Halting pipeline."
        exit 1
    fi

    # 6. RUN VALIDATION STAGE: DBT Tests
    log_message ">>> Starting STEP 6: DBT Quality & Consistency Tests..."
    dbt test --profiles-dir . >> "$LOG_DIR/dbt_test.log" 2>&1
    DBT_TEST_STATUS=$?

    if [ $DBT_TEST_STATUS -eq 0 ]; then
        log_message "SUCCESS: All DBT data quality tests passed successfully."
    else
        log_message "WARNING: Some DBT quality tests failed. Please inspect dbt_test.log!"
    fi
else
    log_message "SKIPPED: DBT directory not found at $DBT_DIR. Skipping transformations."
fi

log_message "=========================================="
log_message ">>> DAILY BI DATA PIPELINE COMPLETED <<<"
log_message "=========================================="
exit 0
