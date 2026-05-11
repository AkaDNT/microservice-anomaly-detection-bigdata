#!/usr/bin/env bash
set -euo pipefail

REPORT_DIR="${1:-reports/gold}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_PATH="$REPORT_DIR/validate_gold_${TIMESTAMP}.log"
LATEST_LOG_PATH="$REPORT_DIR/validate_gold.log"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"

if [[ -x ".venv/bin/spark-submit" ]]; then
  SPARK_SUBMIT=".venv/bin/spark-submit"
fi

mkdir -p "$REPORT_DIR"

{
  echo "Gold validation started at $(date '+%Y-%m-%d %H:%M:%S %z')"
  echo
  "$SPARK_SUBMIT" --driver-memory "$SPARK_DRIVER_MEMORY" src/etl/validate_gold.py
  echo
  echo "Gold validation finished at $(date '+%Y-%m-%d %H:%M:%S %z')"
} 2>&1 | tee "$LOG_PATH"

cp "$LOG_PATH" "$LATEST_LOG_PATH"
echo "Wrote $LOG_PATH"
echo "Updated $LATEST_LOG_PATH"
