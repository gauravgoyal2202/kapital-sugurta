# PowerShell script to create Windows Scheduled Task for daily Ingestion & Transformation Pipeline

$ProjectRoot = "C:\Canopus\project\kp_insurance_git"
$OrchestratorScript = "$ProjectRoot\kapital-sugurta\scripts\orchestration\run_pipeline.ps1"

# 1. Define the action to execute the orchestrator script
$Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File `"$OrchestratorScript`"" -WorkingDirectory $ProjectRoot

# 2. Define daily trigger at 1:00 AM
$Trigger = New-ScheduledTaskTrigger -Daily -At 1am

# 3. Principal: Runs under SYSTEM with administrative privileges
$Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

# 4. Register the Task in Windows Task Scheduler
Register-ScheduledTask -TaskName "Kapital_Insurance_Pipeline_Daily" -Action $Action -Trigger $Trigger -Principal $Principal -Description "Daily sequential execution of Raw Data Ingestion, NPS Loading, Data Quality Checks, and dbt Transformations."

Write-Host "Scheduled task 'Kapital_Insurance_Pipeline_Daily' created successfully."
