#!/bin/bash

# Change directory to the project root
cd "$(dirname "$0")/.." || exit 1

# Configuration
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
# Standard industry practice: Rotate logs by appending the current date to the log file name
LOG_FILE="$LOG_DIR/excel_pipeline_run_$(date '+%Y%m%d').log"

# Function to log messages
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Function to send alert email using standard approach from etl_utils.py
send_alert() {
    local step="$1"
    log "ERROR: Pipeline failed at step: $step"
    
    # Pass Bash values safely into Python through environment variables.
    PIPELINE_FAILED_STEP="$step" \
    PIPELINE_FAILED_TIME="$(date '+%Y-%m-%d %H:%M:%S')" \
    python - <<'PY'
import os
import sys

sys.path.append(os.getcwd())

from src.utils.etl_utils import send_email

step = os.environ.get("PIPELINE_FAILED_STEP", "Unknown step")
failed_time = os.environ.get("PIPELINE_FAILED_TIME", "")

subject = "Excel Pipeline Failure"
body = f"Excel Pipeline failed at step: {step} on {failed_time}"

send_email(subject, body)
PY

    exit 1
}

log "========================================"
log "Starting Excel Pipeline..."

# 0. Pull latest changes from Git
log "Pulling latest changes from git..."
git pull origin refactor/codebase-restructure || send_alert "Git Pull"

# 1. Delete logs older than 15 days in a standard way
log "Cleaning up logs older than 15 days in $LOG_DIR..."
find "$LOG_DIR" -name "excel_pipeline_run_*.log" -type f -mtime +15 -exec rm {} \;

# 2. Activate virtual environment
log "Activating virtual environment..."
source .venv/bin/activate || send_alert "Virtual Environment Activation"

# 3. Extract Excel data and store to Postgres
log "Running extract_solvency.py..."
python src/extract/extract_solvency.py || send_alert "extract_solvency.py"

log "Running extract_nps.py..."
python src/extract/extract_nps.py || send_alert "extract_nps.py"

# log "Running extract_market_share.py..."
# python src/extract/extract_market_share.py || send_alert "extract_market_share.py"

log "Excel Pipeline completed successfully."
log "========================================"
