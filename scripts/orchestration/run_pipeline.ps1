# PowerShell script to orchestrate the complete Ingestion & Transformation Pipeline on Windows

$ProjectRoot = "C:\Canopus\project\kp_insurance_git"
$OrchestrationDir = "$ProjectRoot\kapital-sugurta\scripts\orchestration"
$DbtDir = "$ProjectRoot\kapital-sugurta\dbt\kapital_sugurta_dbt"
$LogDir = "$ProjectRoot\logs"

# Ensure log directory exists
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

$Today = Get-Date -Format "yyyyMMdd"
$LogFile = "$LogDir\pipeline_$Today.log"

function Write-Log {
    param([string]$Message)
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogMsg = "[$Timestamp] $Message"
    Write-Output $LogMsg
    $LogMsg | Out-File -Append -FilePath $LogFile
}

Write-Log "--------------------------------------------------"
Write-Log "Pipeline Ingestion & Transformation started on Windows"

# 1. Full Refresh (Reference Data)
Write-Log "[1/5] Running Full Refresh Jobs..."
python "$OrchestrationDir\job_full_refresh.py" *>> $LogFile
if ($LASTEXITCODE -ne 0) {
    Write-Log "WARNING: Full Refresh Job returned exit code $LASTEXITCODE"
}

# 2. Incremental Refresh (Fact Data)
Write-Log "[2/5] Running Incremental Refresh Jobs..."
python "$OrchestrationDir\job_incremental_refresh.py" *>> $LogFile
if ($LASTEXITCODE -ne 0) {
    Write-Log "WARNING: Incremental Refresh Job returned exit code $LASTEXITCODE"
}

# 3. Data Quality Checks
Write-Log "[3/5] Running Data Quality Validation..."
python "$OrchestrationDir\job_data_quality.py" *>> $LogFile
if ($LASTEXITCODE -ne 0) {
    Write-Log "WARNING: Data Quality Job returned exit code $LASTEXITCODE"
}

# 4. NPS Survey Load (Excel)
Write-Log "[4/5] Loading NPS Survey Data..."
python "$OrchestrationDir\job_load_nps.py" *>> $LogFile
if ($LASTEXITCODE -ne 0) {
    Write-Log "WARNING: NPS Load Job returned exit code $LASTEXITCODE"
}

# 5. dbt Transformations
Write-Log "[5/5] Running dbt Transformations..."
Push-Location $DbtDir
dbt run *>> $LogFile
if ($LASTEXITCODE -ne 0) {
    Write-Log "ERROR: dbt run failed with exit code $LASTEXITCODE"
} else {
    Write-Log "SUCCESS: dbt run completed successfully."
}
Pop-Location

Write-Log "Pipeline finished successfully."
Write-Log "--------------------------------------------------"
