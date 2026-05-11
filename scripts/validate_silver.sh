#!/usr/bin/env bash
set -euo pipefail

TABLES="${1:-logs,metrics,spans,trace_edges,anomalies}"
REPORT_DIR="${2:-reports/silver}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_PATH="$REPORT_DIR/validate_silver_${TIMESTAMP}.log"
LATEST_LOG_PATH="$REPORT_DIR/validate_silver.log"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"

if [[ -x ".venv/bin/spark-submit" ]]; then
  SPARK_SUBMIT=".venv/bin/spark-submit"
fi

mkdir -p "$REPORT_DIR"

{
  echo "Silver validation started at $(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "tables=$TABLES"
  echo
  "$SPARK_SUBMIT" --driver-memory "$SPARK_DRIVER_MEMORY" src/etl/validate_silver.py --tables "$TABLES"
  echo
  echo "Silver validation finished at $(date '+%Y-%m-%d %H:%M:%S %z')"
} 2>&1 | tee "$LOG_PATH"

cp "$LOG_PATH" "$LATEST_LOG_PATH"
echo "Wrote $LOG_PATH"
echo "Updated $LATEST_LOG_PATH"
