#!/bin/bash

export PATH=/usr/bin:/bin:/usr/local/bin

REPO_DIR="$HOME/test/kapital_git/kapital-sugurta/test"
SCRIPT_NAME="script.py"
LOG_FILE="$HOME/test/out_put/cron.log"

# Ensure log directory exists
mkdir -p $HOME/test/out_put

echo "[$(date)] Job started" >> $LOG_FILE

cd $REPO_DIR || {
    echo "Repo directory not found: $REPO_DIR" >> $LOG_FILE
    exit 1
}

git pull origin main >> $LOG_FILE 2>&1

if [ $? -eq 0 ]; then
    echo "Git pull successful" >> $LOG_FILE

    python3 $SCRIPT_NAME >> $LOG_FILE 2>&1

    if [ $? -eq 0 ]; then
        echo "Python script executed successfully" >> $LOG_FILE
    else
        echo "Python script execution FAILED" >> $LOG_FILE
    fi
else
    echo "Git pull FAILED" >> $LOG_FILE
fi

echo "[$(date)] Job finished" >> $LOG_FILE
echo "------------------------" >> $LOG_FILE
