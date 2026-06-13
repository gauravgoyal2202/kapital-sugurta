#!/bin/bash

# Change directory to the project root
cd "$(dirname "$0")/.." || exit 1

# Configuration
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
# Standard industry practice: Rotate logs by appending the current date to the log file name
LOG_FILE="$LOG_DIR/etl_pipeline_run_$(date '+%Y%m%d').log"

# Function to log messages
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Function to send alert email using standard approach from etl_utils.py
send_alert() {
    local step=$1
    log "ERROR: Pipeline failed at step: $step"
    
    # Inline python call to use the project's send_email utility
    python -c "
import sys
import os
# Ensure the project root is in the python path to import scripts
sys.path.append(os.getcwd())
from src.utils.etl_utils import send_email

subject = 'ETL Pipeline Failure'
body = f'ETL Pipeline failed at step: {$step} on $(date '+%Y-%m-%d %H:%M:%S')'
send_email(subject, body)
"
    exit 1
}

log "========================================"
log "Starting ETL Pipeline..."

# 0. Pull latest changes from Git
log "Pulling latest changes from git..."
git pull origin main || send_alert "Git Pull"

# 1. Delete logs older than 15 days in a standard way
log "Cleaning up logs older than 15 days in $LOG_DIR..."
find "$LOG_DIR" -name "etl_pipeline_run_*.log" -type f -mtime +15 -exec rm {} \;

# 2. Activate virtual environment
log "Activating virtual environment..."
source .venv/bin/activate || send_alert "Virtual Environment Activation"

# 3. Fetch data from API
log "Fetching data from API (extract_1c_api.py)..."
python src/extract/extract_1c_api.py || send_alert "API Data Fetch"

# 4. Migrate Oracle tables
log "Migrating Oracle tables (oracle_to_postgres.py)..."
python src/migrate/oracle_to_postgres.py || send_alert "Oracle Migration"

# 5. Run dbt
log "Changing directory to dbt project..."
cd dbt/kapital_sugurta_dbt || send_alert "Changing to dbt directory"

log "Running dbt..."
dbt run || send_alert "dbt run"
log "Run dbt successfully"


log "Changing back to root directory..."
cd ../../

log "ETL Pipeline completed successfully."
log "========================================"
