# PowerShell script to create Windows Scheduled Task for Data Refresh

$Action = New-ScheduledTaskAction -Execute "python.exe" -Argument "C:\Canopus\project\kp_insurance_git\kapital-sugurta\scripts\orchestration\refresh_raw_data.py" -WorkingDirectory "C:\Canopus\project\kp_insurance_git"
$Trigger = New-ScheduledTaskTrigger -Daily -At 1am
$Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName "Kapital_Insurance_Raw_Refresh" -Action $Action -Trigger $Trigger -Principal $Principal -Description "Daily refresh of migrated Oracle tables to Postgres Raw Layer"

# Trigger a dbt run after the refresh (Separate Task or joined Action)
$DbtAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-Command 'cd C:\Canopus\project\kp_insurance_git\kapital-sugurta\dbt\kapital_sugurta_dbt; dbt run'"
$DbtTrigger = New-ScheduledTaskTrigger -Daily -At 4am

Register-ScheduledTask -TaskName "Kapital_Insurance_Dbt_Run" -Action $DbtAction -Trigger $DbtTrigger -Principal $Principal -Description "Daily refresh of Dbt Marts after Raw data refresh"

Write-Host "Scheduled tasks created successfully."
