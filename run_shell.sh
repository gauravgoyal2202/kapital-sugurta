#!/bin/bash

export PATH=/usr/bin:/bin:/usr/local/bin

# Automatically get the directory where this script is located
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$REPO_DIR/logs"
LOG_FILE="$LOG_DIR/pipeline_run.log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

echo "[$(date)] Pipeline job started" >> "$LOG_FILE"

cd "$REPO_DIR" || {
    echo "Repo directory not found: $REPO_DIR" >> "$LOG_FILE"
    exit 1
}

# 1. Pull latest code (optional, you can remove if not needed in cron)
git pull origin main >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "[ERROR] Git pull FAILED" >> "$LOG_FILE"
    # Decide if you want to exit on git pull failure. Commenting out exit so it proceeds anyway.
    # exit 1
else
    echo "[SUCCESS] Git pull successful" >> "$LOG_FILE"
fi

# 2. Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "[INFO] Activated virtual environment (.venv)" >> "$LOG_FILE"
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    echo "[INFO] Activated virtual environment (venv)" >> "$LOG_FILE"
else
    echo "[WARNING] Virtual environment not found. Using system python." >> "$LOG_FILE"
fi

# 3. Run 1C API Load Script
echo "[INFO] Running 1C API Load script..." >> "$LOG_FILE"
python3 scripts/load_1c_api_raw.py >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
    echo "[SUCCESS] 1C API script executed successfully" >> "$LOG_FILE"
else
    echo "[ERROR] 1C API script execution FAILED" >> "$LOG_FILE"
fi

# 4. Run Oracle Migration Script
echo "[INFO] Running Oracle Migration script..." >> "$LOG_FILE"
python3 scripts/migrate_oracle_tables.py >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
    echo "[SUCCESS] Oracle migration script executed successfully" >> "$LOG_FILE"
else
    echo "[ERROR] Oracle migration script execution FAILED" >> "$LOG_FILE"
fi

echo "[$(date)] Pipeline job finished" >> "$LOG_FILE"
echo "--------------------------------------------------------" >> "$LOG_FILE"
