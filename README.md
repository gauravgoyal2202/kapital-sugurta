# Kapital Sugurta Data Platform

An end-to-end Data Warehouse and ETL pipeline for **Kapital Sugurta**. This project extracts data from various source systems (Oracle, 1C APIs, and Google Drive Excel files), loads it into a centralized PostgreSQL Data Warehouse, and transforms it using **dbt (data build tool)** for analytics and BI reporting.

---

## 🏗 Architecture & Stack

- **Extraction / Load (EL):** Python (pandas, psycopg2, oracledb, gdown)
- **Transformation (T):** dbt (Data Build Tool)
- **Data Warehouse:** PostgreSQL
- **Orchestration:** Shell scripts (Bash/Windows)
- **Data Quality & Auditing:** dbt Elementary & custom Postgres metadata logging
- **Alerting:** Automated SMTP Email Alerts on pipeline failure

---

## 📁 Project Structure

The codebase is organized following industry-standard Data Engineering principles:

```text
kapital-sugurta/
│
├── bin/                        # Orchestration shell scripts
│   ├── run_etl_pipeline.sh     # Main Linux ETL pipeline (API & Oracle + dbt)
│   ├── run_excel_pipeline.sh   # Linux ETL for GDrive Excel data
│   └── run_etl_windows.sh      # Windows-compatible run script
│
├── src/                        # Python Source Code
│   ├── extract/                # Scripts to fetch data (APIs, GDrive Excel)
│   ├── migrate/                # High-volume database migrations (Oracle -> Postgres)
│   ├── transform/              # Python-based transformations (if not handled by dbt)
│   └── utils/                  # Shared utilities (DB connections, email alerts, logging)
│
├── dbt/
│   └── kapital_sugurta_dbt/    # dbt project (Models, tests, macros, Elementary)
│
├── data/
│   └── raw/                    # Temporary staging for downloaded Excel files
│
├── logs/                       # Auto-rotating execution logs (retained for 15 days)
│
├── .env                        # Environment variables (Credentials & Config)
└── requirements.txt            # Python dependencies
```

---

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.9+
- PostgreSQL
- Git
- `dbt-postgres`

### 2. Environment Setup
Create a `.env` file in the root directory based on the following template. **Never commit `.env` to version control.**

```env
# Target PostgreSQL (Data Warehouse)
PG_HOST=10.10.3.124
PG_PORT=5432
PG_DATABASE=kapital_insurance_dm
PG_USER=etl_user
PG_PASSWORD=your_secure_password

# Oracle Source
ORACLE_HOST=10.10.3.121
ORACLE_PORT=1521
ORACLE_SERVICE=ORADB
ORACLE_USER=DATA_ANALYST
ORACLE_PASS=data_analyst

# 1C API Source
API_1C_HOST=http://10.10.1.209
API_1C_USER=username
API_1C_PASS=password

# Email Alerting
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=your_email@gmail.com
ALERT_SMTP_PASS=your_app_password
ALERT_TO=alerts_recipient@domain.com

# Google Drive Data Sources
GDRIVE_MARKET_SHARE_LINK="https://drive.google.com/..."
GDRIVE_SOLVENCY_ADEQUACY_LINK="https://drive.google.com/..."
GDRIVE_NPS_LINK="https://drive.google.com/..."
```

### 3. Installation

Create a virtual environment and install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate  # Or `.venv\Scripts\activate` on Windows
pip install -r requirements.txt
```

Initialize dbt packages (Elementary):
```bash
cd dbt/kapital_sugurta_dbt
dbt deps
```

---

## 🔄 Running the Pipelines

Pipelines are orchestrated via shell scripts in the `bin/` directory. They automatically handle environment activation, fetching the latest git changes, log rotation, sequential execution, and email alerting.

**Run the Core ETL (Oracle + API + dbt):**
```bash
./bin/run_etl_pipeline.sh
```

**Run the Excel / Manual Data ETL:**
```bash
./bin/run_excel_pipeline.sh
```

---

## 📊 Logging, Auditing, and Monitoring

We employ a 3-tier approach to pipeline health and observability:

### 1. Text Logs (Debugging)
Every Python script and shell wrapper outputs detailed runtime logs to the `logs/` directory. Logs older than 15 days are automatically pruned.
- View real-time logs: `tail -f logs/etl_pipeline_run_*.log`

### 2. Execution Metadata DB (Pipeline Health)
Every Python extraction script automatically logs its execution state to the PostgreSQL table `raw.etl_refresh_metadata`.
- Tracks `RUNNING`, `SUCCESS`, and `FAILED` states.
- Captures `rows_extracted`, `rows_inserted`, and `duration_seconds`.
- This table can be queried directly by BI tools to monitor extraction health.

### 3. dbt Elementary (Data Quality & Modeling Health)
The project is instrumented with the **Elementary** dbt package. Upon every `dbt run`, Elementary generates an audit trail in the `elementary` schema:
- `elementary.model_run_results`: History and duration of all model builds.
- `elementary.alerts_dbt_tests`: Test failures and data quality anomalies.

---

## 🔔 Alerting

If any step in the shell script orchestration fails (e.g., Python script crashes, network drops, dbt compilation fails), the pipeline will immediately halt, and an email will be dispatched to `ALERT_TO` detailing the exact step of the failure and the timestamp.