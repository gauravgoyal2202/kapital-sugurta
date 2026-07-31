#!/bin/bash
# =============================================================================
#  run_etl_pipeline.sh — Kapital Sugurta Core ETL Pipeline
#  Schedule : 30 20 * * * (daily at 20:30)
#  Steps    : Git pull → Log rotation → 1C API → Oracle → dbt
#  Alerting : Detailed HTML email on every step failure via send_pipeline_alert
# =============================================================================

# ── Change to project root ────────────────────────────────────────────────────
cd "$(dirname "$0")/.." || exit 1

# ── Configuration ─────────────────────────────────────────────────────────────
PIPELINE_NAME="Core ETL Pipeline (run_etl_pipeline.sh)"
LOG_DIR="$(pwd)/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/etl_pipeline_run_$(date '+%Y%m%d').log"
DBT_LOG="$LOG_DIR/dbt_run_$(date '+%Y%m%d_%H%M%S').log"
TMPDIR_ETL="$LOG_DIR/.tmp_etl"
mkdir -p "$TMPDIR_ETL"
PYTHON_BIN="python"
DBT_BIN="dbt"

# ── Logging helper ────────────────────────────────────────────────────────────
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# =============================================================================
#  send_alert STEP ERROR_DETAIL [EXTRA_KEY1 EXTRA_VAL1 ...]
#
#  Calls send_pipeline_alert() in etl_utils.py.
#  Extra key-value pairs are passed as pipe-delimited pairs so Bash doesn't
#  need to export complex arrays.
# =============================================================================
send_alert() {
    local step="$1"
    local error_detail="$2"
    shift 2
    # Remaining args: key1 val1 key2 val2 ...
    local extra_pairs=""
    while [[ $# -ge 2 ]]; do
        extra_pairs+="$1|||$2\n"
        shift 2
    done

    log "ERROR: Pipeline failed at step: [$step]"
    log "Error detail: $error_detail"

    PIPELINE_FAILED_STEP="$step"            \
    PIPELINE_FAILED_ERROR="$error_detail"   \
    PIPELINE_NAME_ENV="$PIPELINE_NAME"      \
    PIPELINE_EXTRA_PAIRS="$extra_pairs"     \
    "$PYTHON_BIN" - <<'PY'
import os, sys
sys.path.insert(0, os.getcwd())
from src.utils.etl_utils import send_pipeline_alert

step         = os.environ.get("PIPELINE_FAILED_STEP",  "Unknown step")
error_detail = os.environ.get("PIPELINE_FAILED_ERROR", "")
pipeline     = os.environ.get("PIPELINE_NAME_ENV",     "Core ETL Pipeline")
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
#  dbt_alert  DBT_STDERR  DBT_LOG_FILE
#
#  Parses dbt JSON run-results to extract failed model names + errors,
#  then calls send_alert with the full structured detail.
# =============================================================================
dbt_alert() {
    local dbt_stderr="$1"
    local dbt_logfile="$2"

    # -- Try to parse run_results.json for failed model names ----------------
    RUN_RESULTS="dbt/kapital_sugurta_dbt/target/run_results.json"
    local failed_models_text=""
    if [[ -f "$RUN_RESULTS" ]]; then
        failed_models_text=$("$PYTHON_BIN" - "$RUN_RESULTS" <<'PY'
import sys, json, traceback
try:
    with open(sys.argv[1]) as f:
        data = json.load(f)
    failures = []
    for r in data.get("results", []):
        if r.get("status") in ("error", "fail", "runtime error"):
            node = r.get("unique_id", "unknown").replace("model.", "").replace(".", " → ")
            msg  = r.get("message", "") or r.get("adapter_response", {}).get("_message", "")
            failures.append(f"  • {node}\n    {msg}")
    if failures:
        print(f"{len(failures)} model(s) failed:\n" + "\n".join(failures))
    else:
        print("(run_results.json found but no individual model failures detected)")
except Exception as e:
    print(f"(Could not parse run_results.json: {e})")
PY
        )
    fi

    # -- Combine stderr + parsed failures ------------------------------------
    local combined_error=""
    if [[ -n "$failed_models_text" ]]; then
        combined_error="${failed_models_text}"$'\n\n'"--- dbt stderr ---"$'\n'"${dbt_stderr}"
    else
        combined_error="${dbt_stderr}"
    fi

    send_alert "dbt Run" "$combined_error" \
        "dbt Log File" "$dbt_logfile"      \
        "run_results.json" "$RUN_RESULTS"
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
for logfile in migrate_oracle extract_1c daily_pipeline_cron; do
    if [[ -f "$LOG_DIR/${logfile}.log" ]]; then
        cp "$LOG_DIR/${logfile}.log" "$LOG_DIR/${logfile}_$(date '+%Y%m%d').log"
        > "$LOG_DIR/${logfile}.log"
    fi
done

find "$LOG_DIR" -name "etl_pipeline_run_*.log" -type f -mtime +7 -exec rm {} \;
if [[ $? -ne 0 ]]; then
    log "Step 1 (etl_pipeline_run logs) failed. Retrying in 10 seconds..."
    sleep 10
    find "$LOG_DIR" -name "etl_pipeline_run_*.log" -type f -mtime +7 -exec rm {} \;
fi
find "$LOG_DIR" -name "dbt_run_*.log"          -type f -mtime +7 -exec rm {} \;
if [[ $? -ne 0 ]]; then
    log "Step 1 (dbt_run logs) failed. Retrying in 10 seconds..."
    sleep 10
    find "$LOG_DIR" -name "dbt_run_*.log"          -type f -mtime +7 -exec rm {} \;
fi
find "$LOG_DIR" -name "migrate_oracle_*.log"   -type f -mtime +7 -exec rm {} \;
find "$LOG_DIR" -name "extract_1c_*.log"       -type f -mtime +7 -exec rm {} \;
find "$LOG_DIR" -name "daily_pipeline_cron_*.log" -type f -mtime +7 -exec rm {} \;

# Clean up cumulative dbt.log and any misplaced run logs inside dbt/kapital_sugurta_dbt/logs
rm -f dbt/kapital_sugurta_dbt/logs/dbt.log 2>/dev/null
find dbt/kapital_sugurta_dbt/logs -name "etl_pipeline_run_*.log" -type f -delete 2>/dev/null
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
    PYTHON_BIN="$(pwd)/.venv/Scripts/python"
elif [ -f ".venv/Scripts/python.exe" ]; then
    PYTHON_BIN="$(pwd)/.venv/Scripts/python.exe"
elif [ -f ".venv/bin/python" ]; then
    PYTHON_BIN="$(pwd)/.venv/bin/python"
fi

if [ -f ".venv/Scripts/dbt.exe" ]; then
    DBT_BIN="$(pwd)/.venv/Scripts/dbt.exe"
elif [ -f ".venv/Scripts/dbt" ]; then
    DBT_BIN="$(pwd)/.venv/Scripts/dbt"
elif [ -f ".venv/bin/dbt" ]; then
    DBT_BIN="$(pwd)/.venv/bin/dbt"
fi

log "[Step 2] Virtual environment activated OK (Using Python: $PYTHON_BIN, dbt: $DBT_BIN)."

# ── Step 3 : 1C API extraction ────────────────────────────────────────────────
log "[Step 3] Fetching data from 1C API (extract_1c_api.py)..."
STEP3_STDERR_FILE="$TMPDIR_ETL/step3_1capi.err"
"$PYTHON_BIN" src/extract/extract_1c_api.py > >(tee -a "$LOG_FILE") 2>"$STEP3_STDERR_FILE"
STEP3_EXIT=$?
if [[ $STEP3_EXIT -ne 0 ]]; then
    log "Step 3 failed. Retrying in 10 seconds..."
    sleep 10
    > "$STEP3_STDERR_FILE"
    "$PYTHON_BIN" src/extract/extract_1c_api.py > >(tee -a "$LOG_FILE") 2>"$STEP3_STDERR_FILE"
    STEP3_EXIT=$?
fi

STEP3_ERR=$(cat "$STEP3_STDERR_FILE")
if [[ $STEP3_EXIT -ne 0 ]]; then
    # Fallback to reading logs if stderr is empty (since logging StreamHandler defaults to stdout)
    if [[ -z "$STEP3_ERR" ]]; then
        if [[ -f "logs/extract_1c.log" ]]; then
            STEP3_ERR=$(tail -n 25 logs/extract_1c.log)
        else
            STEP3_ERR="No stderr captured. Last log lines:\n$(tail -n 25 "$LOG_FILE")"
        fi
    fi
    # Detect specific connection / auth failure keywords for better context
    API_EXTRA=""
    if echo "$STEP3_ERR" | grep -qi "connection refused\|connect timeout\|name or service not known"; then
        API_EXTRA="API_NOTE=Cannot reach 1C API host. Check that VPN is connected and host http://10.10.1.209 is accessible."
    elif echo "$STEP3_ERR" | grep -qi "401\|unauthorized\|forbidden\|403"; then
        API_EXTRA="API_NOTE=HTTP authentication failed. Verify API_1C_USER and API_1C_PASS in .env."
    fi
    send_alert "1C API Extraction" "$STEP3_ERR" \
        "Script" "src/extract/extract_1c_api.py" \
        "API Host" "${API_1C_HOST:-http://10.10.1.209}" \
        "Hint" "${API_EXTRA:-Check logs for details}"
fi
log "[Step 3] 1C API extraction OK."

# ── Step 4 : Oracle → PostgreSQL migration ────────────────────────────────────
log "[Step 4] Migrating Oracle tables to PostgreSQL (oracle_to_postgres.py)..."
STEP4_STDERR_FILE="$TMPDIR_ETL/step4_oracle.err"
"$PYTHON_BIN" src/migrate/oracle_to_postgres.py > >(tee -a "$LOG_FILE") 2>"$STEP4_STDERR_FILE"
STEP4_EXIT=$?
if [[ $STEP4_EXIT -ne 0 ]]; then
    log "Step 4 failed. Retrying in 10 seconds..."
    sleep 10
    > "$STEP4_STDERR_FILE"
    "$PYTHON_BIN" src/migrate/oracle_to_postgres.py > >(tee -a "$LOG_FILE") 2>"$STEP4_STDERR_FILE"
    STEP4_EXIT=$?
fi

STEP4_ERR=$(cat "$STEP4_STDERR_FILE")
if [[ $STEP4_EXIT -ne 0 ]]; then
    # Fallback to reading logs if stderr is empty
    if [[ -z "$STEP4_ERR" ]]; then
        if [[ -f "logs/migrate_oracle.log" ]]; then
            STEP4_ERR=$(tail -n 25 logs/migrate_oracle.log)
        else
            STEP4_ERR="No stderr captured. Last log lines:\n$(tail -n 25 "$LOG_FILE")"
        fi
    fi
    # Classify the Oracle/PG error type for a more helpful message
    HINT=""
    if echo "$STEP4_ERR" | grep -qi "ORA-12170\|ORA-12541\|ORA-12543\|ORA-12535\|TNS:"; then
        HINT="Cannot connect to Oracle DB. Check that host 10.10.3.121:1521 is reachable and service ORADB is running."
    elif echo "$STEP4_ERR" | grep -qi "ORA-01017\|invalid username/password"; then
        HINT="Oracle login failed. Verify ORACLE_USER and ORACLE_PASS in .env."
    elif echo "$STEP4_ERR" | grep -qi "ORA-00942\|table or view does not exist"; then
        HINT="Oracle source table/view not found. The schema KAPITALDB may have changed or the user lacks SELECT privilege."
    elif echo "$STEP4_ERR" | grep -qi "ORA-00904\|invalid identifier"; then
        HINT="Oracle column name not found. A source schema change may have removed or renamed a column."
    elif echo "$STEP4_ERR" | grep -qi "does not exist\|schema.*not found\|no schema"; then
        HINT="PostgreSQL target schema not found. Verify that schemas 'raw', 'curated', 'presentation' exist in kapital_insurance_dm."
    elif echo "$STEP4_ERR" | grep -qi "connection refused\|could not connect to server\|pg.*timeout"; then
        HINT="Cannot connect to PostgreSQL. Check that host 10.10.3.124:5432 is reachable and DB kapital_insurance_dm is running."
    elif echo "$STEP4_ERR" | grep -qi "password authentication failed"; then
        HINT="PostgreSQL login failed. Verify PG_USER and PG_PASSWORD in .env."
    else
        HINT="See full error detail below."
    fi

    send_alert "Oracle → PostgreSQL Migration" "$STEP4_ERR" \
        "Script"          "src/migrate/oracle_to_postgres.py" \
        "Oracle Host"     "${ORACLE_HOST:-10.10.3.121}:${ORACLE_PORT:-1521}" \
        "Oracle Service"  "${ORACLE_SERVICE:-ORADB}" \
        "PG Host"         "${PG_HOST:-10.10.3.124}:${PG_PORT:-5432}" \
        "PG Database"     "${PG_DATABASE:-kapital_insurance_dm}" \
        "Diagnosis"       "$HINT"
fi
log "[Step 4] Oracle migration OK."

# ── Step 5 : dbt run ──────────────────────────────────────────────────────────
log "[Step 5] Running dbt models..."
cd dbt/kapital_sugurta_dbt
if [[ $? -ne 0 ]]; then
    log "Failed to cd into dbt/kapital_sugurta_dbt. Retrying in 10 seconds..."
    sleep 10
    cd dbt/kapital_sugurta_dbt
    if [[ $? -ne 0 ]]; then
        send_alert "dbt — Change Directory" \
            "Could not cd into dbt/kapital_sugurta_dbt. Folder may be missing." \
            "Expected path" "$(pwd)/dbt/kapital_sugurta_dbt"
    fi
fi

# Run dbt; capture both stdout (to log file) and stderr (for alert parsing)
STEP5_STDERR_FILE="$TMPDIR_ETL/step5_dbt.err"
"$DBT_BIN" run 2>"$STEP5_STDERR_FILE" | tee -a "$LOG_FILE"
STEP5_EXIT=${PIPESTATUS[0]}

if [[ $STEP5_EXIT -ne 0 ]]; then
    log "Step 5 failed. Retrying in 10 seconds..."
    sleep 10
    > "$STEP5_STDERR_FILE"
    "$DBT_BIN" run 2>"$STEP5_STDERR_FILE" | tee -a "$LOG_FILE"
    STEP5_EXIT=${PIPESTATUS[0]}
fi

STEP5_ERR=$(cat "$STEP5_STDERR_FILE")

if [[ $STEP5_EXIT -ne 0 ]]; then
    cd ../../
    dbt_alert "$STEP5_ERR" "$(realpath "$DBT_LOG" 2>/dev/null || echo "$DBT_LOG")"
    # dbt_alert calls send_alert which calls exit 1
fi

log "[Step 5] dbt run OK."
cd ../../

# ── Cleanup temp dir ──────────────────────────────────────────────────────────
rm -rf "$TMPDIR_ETL"

log "========================================================================"
log "  $PIPELINE_NAME  completed successfully."
log "========================================================================"
