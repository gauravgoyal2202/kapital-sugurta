#!/bin/bash
# =============================================================================
#  run_excel_pipeline.sh — Kapital Sugurta Excel / Google Drive ETL Pipeline
#  Schedule : 30 19 * * * (daily at 19:30, runs 1 hour before Core ETL)
#  Steps    : Git pull → Log rotation → Solvency → NPS (→ Market Share)
#  Alerting : Detailed HTML email on every step failure via send_pipeline_alert
# =============================================================================

# ── Change to project root ────────────────────────────────────────────────────
cd "$(dirname "$0")/.." || exit 1

# ── Configuration ─────────────────────────────────────────────────────────────
PIPELINE_NAME="Excel / Google Drive ETL Pipeline (run_excel_pipeline.sh)"
LOG_DIR="$(pwd)/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/excel_pipeline_run_$(date '+%Y%m%d').log"
TMPDIR_ETL="$LOG_DIR/.tmp_excel"
mkdir -p "$TMPDIR_ETL"
PYTHON_BIN="python"

# ── Logging helper ────────────────────────────────────────────────────────────
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# =============================================================================
#  send_alert STEP ERROR_DETAIL [EXTRA_KEY1 EXTRA_VAL1 ...]
# =============================================================================
send_alert() {
    local step="$1"
    local error_detail="$2"
    shift 2
    local extra_pairs=""
    while [[ $# -ge 2 ]]; do
        extra_pairs+="$1|||$2\n"
        shift 2
    done

    log "ERROR: Pipeline failed at step: [$step]"
    log "Error detail: $error_detail"

    PIPELINE_FAILED_STEP="$step"           \
    PIPELINE_FAILED_ERROR="$error_detail"  \
    PIPELINE_NAME_ENV="$PIPELINE_NAME"     \
    PIPELINE_EXTRA_PAIRS="$extra_pairs"    \
    "$PYTHON_BIN" - <<'PY'
import os, sys
sys.path.insert(0, os.getcwd())
from src.utils.etl_utils import send_pipeline_alert

step         = os.environ.get("PIPELINE_FAILED_STEP",  "Unknown step")
error_detail = os.environ.get("PIPELINE_FAILED_ERROR", "")
pipeline     = os.environ.get("PIPELINE_NAME_ENV",     "Excel ETL Pipeline")
raw_pairs    = os.environ.get("PIPELINE_EXTRA_PAIRS",  "")

extra_rows = []
for line in raw_pairs.split("\\n"):
    line = line.strip()
    if "|||" in line:
        k, v = line.split("|||", 1)
        extra_rows.append((k.strip(), v.strip()))

send_pipeline_alert(
    step_name    = step,
    error_detail = error_detail,
    pipeline_name= pipeline,
    extra_rows   = extra_rows or None,
)
PY
    exit 1
}


# =============================================================================
#  PIPELINE START
# =============================================================================
log "========================================================================"
log "  $PIPELINE_NAME  starting..."
log "========================================================================"

# ── Step 0 : Git pull ─────────────────────────────────────────────────────────
log "[Step 0] Pulling latest code from git (branch: main)..."
STEP0_ERR=$(git pull origin main 2>&1 >/dev/null)
if [[ $? -ne 0 ]]; then
    log "Step 0 failed. Retrying in 10 seconds..."
    sleep 10
    STEP0_ERR=$(git pull origin main 2>&1 >/dev/null)
    if [[ $? -ne 0 ]]; then
        send_alert "Git Pull" "$STEP0_ERR" \
            "Branch" "main" \
            "Remote" "origin"
    fi
fi
log "[Step 0] Git pull OK."

# ── Step 1 : Log rotation ─────────────────────────────────────────────────────
log "[Step 1] Rotating logs older than 7 days..."

# Rotate cumulative log files into daily snapshots
for logfile in load_solvency_adequacy load_nps daily_pipeline_cron; do
    if [[ -f "$LOG_DIR/${logfile}.log" ]]; then
        cp "$LOG_DIR/${logfile}.log" "$LOG_DIR/${logfile}_$(date '+%Y%m%d').log"
        > "$LOG_DIR/${logfile}.log"
    fi
done

find "$LOG_DIR" -name "excel_pipeline_run_*.log" -type f -mtime +7 -exec rm {} \;
if [[ $? -ne 0 ]]; then
    log "Step 1 failed. Retrying in 10 seconds..."
    sleep 10
    find "$LOG_DIR" -name "excel_pipeline_run_*.log" -type f -mtime +7 -exec rm {} \;
fi
find "$LOG_DIR" -name "load_solvency_adequacy_*.log" -type f -mtime +7 -exec rm {} \;
find "$LOG_DIR" -name "load_nps_*.log"               -type f -mtime +7 -exec rm {} \;
find "$LOG_DIR" -name "daily_pipeline_cron_*.log"    -type f -mtime +7 -exec rm {} \;
log "[Step 1] Log rotation OK."

# ── Step 2 : Activate virtual environment ─────────────────────────────────────
log "[Step 2] Activating Python virtual environment..."
STEP2_ERR=$(source .venv/bin/activate 2>&1)
if [[ $? -ne 0 ]]; then
    log "Step 2 failed. Retrying in 10 seconds..."
    sleep 10
    STEP2_ERR=$(source .venv/bin/activate 2>&1)
    if [[ $? -ne 0 ]]; then
        send_alert "Virtual Environment Activation" \
            "Failed to activate .venv/bin/activate\n\n$STEP2_ERR" \
            "Expected path" "$(pwd)/.venv/bin/activate"
    fi
fi

if [ -f ".venv/Scripts/python" ]; then
    PYTHON_BIN=".venv/Scripts/python"
elif [ -f ".venv/Scripts/python.exe" ]; then
    PYTHON_BIN=".venv/Scripts/python.exe"
elif [ -f ".venv/bin/python" ]; then
    PYTHON_BIN=".venv/bin/python"
fi

log "[Step 2] Virtual environment activated OK (Using Python: $PYTHON_BIN)."

# ── Step 3 : Solvency Adequacy extraction ────────────────────────────────────
log "[Step 3] Running extract_solvency.py (Google Drive → raw.solvency_adequacy)..."
STEP3_STDERR_FILE="$TMPDIR_ETL/step3_solvency.err"
"$PYTHON_BIN" src/extract/extract_solvency.py > >(tee -a "$LOG_FILE") 2>"$STEP3_STDERR_FILE"
STEP3_EXIT=$?
if [[ $STEP3_EXIT -ne 0 ]]; then
    log "Step 3 failed. Retrying in 10 seconds..."
    sleep 10
    > "$STEP3_STDERR_FILE"
    "$PYTHON_BIN" src/extract/extract_solvency.py > >(tee -a "$LOG_FILE") 2>"$STEP3_STDERR_FILE"
    STEP3_EXIT=$?
fi

STEP3_ERR=$(cat "$STEP3_STDERR_FILE")
if [[ $STEP3_EXIT -ne 0 ]]; then
    # Fallback to reading logs if stderr is empty
    if [[ -z "$STEP3_ERR" ]]; then
        if [[ -f "logs/load_solvency_adequacy.log" ]]; then
            STEP3_ERR=$(tail -n 25 logs/load_solvency_adequacy.log)
        else
            STEP3_ERR="No stderr captured. Last log lines:\n$(tail -n 25 "$LOG_FILE")"
        fi
    fi
    HINT="See error detail below."
    if echo "$STEP3_ERR" | grep -qi "google\|gdown\|403\|drive\|permission"; then
        HINT="Google Drive download failed. Verify GDRIVE_SOLVENCY_ADEQUACY_LINK in .env and ensure the folder is publicly accessible."
    elif echo "$STEP3_ERR" | grep -qi "connection refused\|could not connect\|timeout"; then
        HINT="Cannot reach PostgreSQL. Verify PG_HOST/PG_PORT/PG_DATABASE in .env."
    elif echo "$STEP3_ERR" | grep -qi "password authentication\|authentication failed"; then
        HINT="PostgreSQL login failed. Verify PG_USER and PG_PASSWORD in .env."
    fi
    send_alert "Solvency Adequacy Extraction" "$STEP3_ERR" \
        "Script"      "src/extract/extract_solvency.py" \
        "Target Table" "raw.solvency_adequacy" \
        "Diagnosis"   "$HINT"
fi
log "[Step 3] Solvency extraction OK."

# ── Step 4 : NPS Survey extraction ───────────────────────────────────────────
log "[Step 4] Running extract_nps.py (Google Drive → raw.nps_survey_responses)..."
STEP4_STDERR_FILE="$TMPDIR_ETL/step4_nps.err"
"$PYTHON_BIN" src/extract/extract_nps.py > >(tee -a "$LOG_FILE") 2>"$STEP4_STDERR_FILE"
STEP4_EXIT=$?
if [[ $STEP4_EXIT -ne 0 ]]; then
    log "Step 4 failed. Retrying in 10 seconds..."
    sleep 10
    > "$STEP4_STDERR_FILE"
    "$PYTHON_BIN" src/extract/extract_nps.py > >(tee -a "$LOG_FILE") 2>"$STEP4_STDERR_FILE"
    STEP4_EXIT=$?
fi

STEP4_ERR=$(cat "$STEP4_STDERR_FILE")
if [[ $STEP4_EXIT -ne 0 ]]; then
    # Fallback to reading logs if stderr is empty
    if [[ -z "$STEP4_ERR" ]]; then
        if [[ -f "logs/load_nps.log" ]]; then
            STEP4_ERR=$(tail -n 25 logs/load_nps.log)
        else
            STEP4_ERR="No stderr captured. Last log lines:\n$(tail -n 25 "$LOG_FILE")"
        fi
    fi
    HINT="See error detail below."
    if echo "$STEP3_ERR" | grep -qi "google\|gdown\|403\|drive\|permission"; then
        HINT="Google Drive download failed. Verify GDRIVE_NPS_LINK in .env and ensure the folder is publicly accessible."
    elif echo "$STEP3_ERR" | grep -qi "connection refused\|could not connect\|timeout"; then
        HINT="Cannot reach PostgreSQL. Verify PG_HOST/PG_PORT/PG_DATABASE in .env."
    fi
    send_alert "NPS Survey Extraction" "$STEP4_ERR" \
        "Script"      "src/extract/extract_nps.py" \
        "Target Table" "raw.nps_survey_responses" \
        "Diagnosis"   "$HINT"
fi
log "[Step 4] NPS extraction OK."

# ── Step 5 : Market Share extraction (currently disabled) ─────────────────────
# Uncomment the block below to re-enable market share extraction
# log "[Step 5] Running extract_market_share.py..."
# STEP5_STDERR_FILE="$TMPDIR_ETL/step5_market.err"
# python src/extract/extract_market_share.py > >(tee -a "$LOG_FILE") 2>"$STEP5_STDERR_FILE"
# STEP5_EXIT=$?
# STEP5_ERR=$(cat "$STEP5_STDERR_FILE")
# if [[ $STEP5_EXIT -ne 0 ]]; then
#     send_alert "Market Share Extraction" "$STEP5_ERR" \
#         "Script"      "src/extract/extract_market_share.py" \
#         "Target Table" "raw.market_share_insurance_class_stats"
# fi
# log "[Step 5] Market share extraction OK."

# ── Cleanup ───────────────────────────────────────────────────────────────────
rm -rf "$TMPDIR_ETL"

log "========================================================================"
log "  $PIPELINE_NAME  completed successfully."
log "========================================================================"
