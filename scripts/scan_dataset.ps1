param(
    [string]$RawRoot = "data/raw/train-ticket",
    [string]$ReportDir = "reports/inventory"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $RawRoot)) {
    throw "Raw data root not found: $RawRoot"
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$rawRootItem = Get-Item -LiteralPath $RawRoot
$caseDirs = Get-ChildItem -LiteralPath $RawRoot -Directory |
    Where-Object { $_.Name -like "case_*" } |
    Sort-Object Name

$rows = foreach ($caseDir in $caseDirs) {
    $files = Get-ChildItem -LiteralPath $caseDir.FullName -Recurse -File
    $logStructuredFiles = $files | Where-Object { $_.Name -like "LOGS_*_structured.csv" }
    $logRawFiles = $files | Where-Object { $_.Name -like "LOGS_*.txt" -and $_.Name -notlike "*_structured.csv" -and $_.Name -notlike "*_templates.csv" }
    $templateFiles = $files | Where-Object { $_.Name -like "LOGS_*_templates.csv" }
    $anomalyFiles = $files | Where-Object { $_.Name -like "potentialAnomalies_*.txt" }
    $monitoringFiles = $files | Where-Object { $_.FullName -match "\\Monitoring_" -and $_.Extension -eq ".json" }
    $traceFiles = $files | Where-Object { $_.FullName -match "\\Traces_" -and $_.Extension -eq ".json" }

    $logRows = 0
    foreach ($logFile in $logStructuredFiles) {
        $lineCount = (Get-Content -LiteralPath $logFile.FullName | Measure-Object -Line).Lines
        if ($lineCount -gt 0) {
            $logRows += ($lineCount - 1)
        }
    }

    [PSCustomObject]@{
        case_id = $caseDir.Name
        raw_files = $files.Count
        raw_log_files = $logRawFiles.Count
        structured_log_files = $logStructuredFiles.Count
        structured_log_rows = $logRows
        log_template_files = $templateFiles.Count
        monitoring_json_files = $monitoringFiles.Count
        trace_json_files = $traceFiles.Count
        anomaly_files = $anomalyFiles.Count
        size_mb = [Math]::Round((($files | Measure-Object Length -Sum).Sum / 1MB), 2)
    }
}

$csvPath = Join-Path $ReportDir "dataset_inventory.csv"
$mdPath = Join-Path $ReportDir "dataset_inventory.md"

$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

$totalFiles = ($rows | Measure-Object raw_files -Sum).Sum
$totalLogRows = ($rows | Measure-Object structured_log_rows -Sum).Sum
$totalSizeMb = [Math]::Round((($rows | Measure-Object size_mb -Sum).Sum), 2)
$generatedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"

$markdown = New-Object System.Collections.Generic.List[string]
$markdown.Add("# Dataset Inventory")
$markdown.Add("")
$markdown.Add("Generated at: $generatedAt")
$markdown.Add("")
$markdown.Add("Raw root: $RawRoot")
$markdown.Add("")
$markdown.Add("## Summary")
$markdown.Add("")
$markdown.Add("| Metric | Value |")
$markdown.Add("|---|---:|")
$markdown.Add("| Cases | $($rows.Count) |")
$markdown.Add("| Raw files | $totalFiles |")
$markdown.Add("| Structured log rows | $totalLogRows |")
$markdown.Add("| Approx size MB | $totalSizeMb |")
$markdown.Add("")
$markdown.Add("## Cases")
$markdown.Add("")
$markdown.Add("| Case | Raw files | Structured logs | Log rows | Metrics JSON | Trace JSON | Anomaly files | Size MB |")
$markdown.Add("|---|---:|---:|---:|---:|---:|---:|---:|")

foreach ($row in $rows) {
    $markdown.Add("| $($row.case_id) | $($row.raw_files) | $($row.structured_log_files) | $($row.structured_log_rows) | $($row.monitoring_json_files) | $($row.trace_json_files) | $($row.anomaly_files) | $($row.size_mb) |")
}

$markdown | Set-Content -Path $mdPath -Encoding UTF8

Write-Host "Wrote $csvPath"
Write-Host "Wrote $mdPath"
