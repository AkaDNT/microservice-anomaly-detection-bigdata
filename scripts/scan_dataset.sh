#!/usr/bin/env bash
set -euo pipefail

RAW_ROOT="${1:-data/raw/train-ticket}"
REPORT_DIR="${2:-reports/inventory}"

if [[ ! -d "$RAW_ROOT" ]]; then
  echo "Raw data root not found: $RAW_ROOT" >&2
  exit 1
fi

mkdir -p "$REPORT_DIR"

CSV_PATH="$REPORT_DIR/dataset_inventory.csv"
MD_PATH="$REPORT_DIR/dataset_inventory.md"

printf '"case_id","raw_files","raw_log_files","structured_log_files","structured_log_rows","log_template_files","monitoring_json_files","trace_json_files","anomaly_files","size_mb"\n' > "$CSV_PATH"

total_cases=0
total_files=0
total_log_rows=0
total_size_kb=0

rows_md=()

while IFS= read -r -d '' case_dir; do
  case_id="$(basename "$case_dir")"
  total_cases=$((total_cases + 1))

  raw_files="$(find "$case_dir" -type f | wc -l | tr -d ' ')"
  raw_log_files="$(find "$case_dir" -type f -name 'LOGS_*.txt' ! -name '*_structured.csv' ! -name '*_templates.csv' | wc -l | tr -d ' ')"
  structured_log_files="$(find "$case_dir" -type f -name 'LOGS_*_structured.csv' | wc -l | tr -d ' ')"
  log_template_files="$(find "$case_dir" -type f -name 'LOGS_*_templates.csv' | wc -l | tr -d ' ')"
  monitoring_json_files="$(find "$case_dir" -type f -path '*/Monitoring_*/*.json' | wc -l | tr -d ' ')"
  trace_json_files="$(find "$case_dir" -type f -path '*/Traces_*/*.json' | wc -l | tr -d ' ')"
  anomaly_files="$(find "$case_dir" -type f -name 'potentialAnomalies_*.txt' | wc -l | tr -d ' ')"

  structured_log_rows=0
  while IFS= read -r -d '' log_file; do
    line_count="$(wc -l < "$log_file" | tr -d ' ')"
    if [[ "$line_count" -gt 0 ]]; then
      structured_log_rows=$((structured_log_rows + line_count - 1))
    fi
  done < <(find "$case_dir" -type f -name 'LOGS_*_structured.csv' -print0)

  size_kb="$(du -sk "$case_dir" | awk '{print $1}')"
  size_mb="$(awk -v kb="$size_kb" 'BEGIN { printf "%.2f", kb / 1024 }')"

  total_files=$((total_files + raw_files))
  total_log_rows=$((total_log_rows + structured_log_rows))
  total_size_kb=$((total_size_kb + size_kb))

  printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
    "$case_id" "$raw_files" "$raw_log_files" "$structured_log_files" "$structured_log_rows" \
    "$log_template_files" "$monitoring_json_files" "$trace_json_files" "$anomaly_files" "$size_mb" >> "$CSV_PATH"

  rows_md+=("| $case_id | $raw_files | $structured_log_files | $structured_log_rows | $monitoring_json_files | $trace_json_files | $anomaly_files | $size_mb |")
done < <(find "$RAW_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'case_*' -print0 | sort -z)

total_size_mb="$(awk -v kb="$total_size_kb" 'BEGIN { printf "%.2f", kb / 1024 }')"
generated_at="$(date '+%Y-%m-%d %H:%M:%S %z')"

{
  echo "# Dataset Inventory"
  echo
  echo "Generated at: $generated_at"
  echo
  echo "Raw root: \`$RAW_ROOT\`"
  echo
  echo "## Summary"
  echo
  echo "| Metric | Value |"
  echo "|---|---:|"
  echo "| Cases | $total_cases |"
  echo "| Raw files | $total_files |"
  echo "| Structured log rows | $total_log_rows |"
  echo "| Approx size MB | $total_size_mb |"
  echo
  echo "## Cases"
  echo
  echo "| Case | Raw files | Structured logs | Log rows | Metrics JSON | Trace JSON | Anomaly files | Size MB |"
  echo "|---|---:|---:|---:|---:|---:|---:|---:|"
  printf '%s\n' "${rows_md[@]}"
} > "$MD_PATH"

echo "Wrote $CSV_PATH"
echo "Wrote $MD_PATH"
