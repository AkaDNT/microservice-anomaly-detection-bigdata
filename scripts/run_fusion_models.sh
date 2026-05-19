#!/usr/bin/env bash
set -euo pipefail

REPORT_DIR="reports/models"
if [[ $# -gt 0 && "$1" != --* ]]; then
  REPORT_DIR="$1"
  shift
fi
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_PATH="$REPORT_DIR/train_fusion_${TIMESTAMP}.log"
LATEST_LOG_PATH="$REPORT_DIR/train_fusion.log"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-6g}"

if [[ -x ".venv/bin/spark-submit" ]]; then
  SPARK_SUBMIT=".venv/bin/spark-submit"
fi

mkdir -p "$REPORT_DIR" reports/metrics

CMD=(
  "$SPARK_SUBMIT"
  --driver-memory "$SPARK_DRIVER_MEMORY"
  --conf spark.driver.maxResultSize=1g
  --conf spark.sql.shuffle.partitions=4
  src/models/train_fusion.py
  "$@"
)

{
  echo "Fusion training started at $(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "command=${CMD[*]}"
  echo
  "${CMD[@]}"
  echo
  echo "Fusion training finished at $(date '+%Y-%m-%d %H:%M:%S %z')"
} 2>&1 | tee "$LOG_PATH"

cp "$LOG_PATH" "$LATEST_LOG_PATH"
echo "Wrote $LOG_PATH"
echo "Updated $LATEST_LOG_PATH"
