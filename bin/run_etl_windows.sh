#!/usr/bin/env bash
set -o pipefail

# Change directory to the project root.
# This assumes this file is inside a folder like scripts/run_etl_windows.sh
cd "$(dirname "$0")/.." || exit 1

# Configuration
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/etl_pipeline_run_$(date '+%Y%m%d').log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

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

subject = "ETL Pipeline Failure"
body = f"ETL Pipeline failed at step: {step} on {failed_time}"

send_email(subject, body)
PY

    exit 1
}

log "========================================"
log "Starting ETL Pipeline..."

# 0. Pull latest changes from Git
log "Pulling latest changes from git..."
git pull origin refactor/codebase-restructure || send_alert "Git Pull"

# 1. Delete logs older than 15 days
log "Cleaning up logs older than 15 days in $LOG_DIR..."
find "$LOG_DIR" -name "etl_pipeline_run_*.log" -type f -mtime +15 -exec rm {} \;

# 2. Activate virtual environment
log "Activating virtual environment..."

if [ -f ".venv/Scripts/activate" ]; then
    # Windows virtual environment, Git Bash
    source .venv/Scripts/activate || send_alert "Virtual Environment Activation"
elif [ -f ".venv/bin/activate" ]; then
    # Linux/macOS/WSL virtual environment
    source .venv/bin/activate || send_alert "Virtual Environment Activation"
else
    send_alert "Virtual Environment Not Found"
fi

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

log "Changing back to root directory..."
cd ../.. || send_alert "Changing back to root directory"

log "ETL Pipeline completed successfully."
log "========================================"
