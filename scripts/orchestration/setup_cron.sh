#!/bin/bash

# Script to register daily Ingestion & Transformation Pipeline in Ubuntu Crontab using Client Paths

# 1. Define exact client paths
PIPELINE_SCRIPT="/home/kapital/kapital_bi_pipeline/etl/jobs/run_pipeline.sh"

echo "Configuring Cron Job for client VM using exact path: $PIPELINE_SCRIPT"

# 2. Make the pipeline script executable
if [ -f "$PIPELINE_SCRIPT" ]; then
    chmod +x "$PIPELINE_SCRIPT"
else
    echo "WARNING: Script not found at $PIPELINE_SCRIPT yet. We will still schedule it."
fi

# 3. Create the crontab command string (Daily at 1:00 AM)
CRON_TIME="0 1 * * *"
CRON_JOB="$CRON_TIME /bin/bash $PIPELINE_SCRIPT"

# 4. Check if the cron job already exists to prevent duplicate entries
(crontab -l 2>/dev/null | grep -Fq "$PIPELINE_SCRIPT")

if [ $? -eq 0 ]; then
    echo "Task is already scheduled in crontab. No changes made."
else
    # 5. Append the cron job safely
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "SUCCESS: Cron job created to run daily at 1:00 AM."
    echo "You can check scheduled tasks by running: crontab -l"
fi
