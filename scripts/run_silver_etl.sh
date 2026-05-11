#!/usr/bin/env bash
set -euo pipefail

SOURCE="${1:-all}"
CASES="${2:-}"
REPORT_DIR="${3:-reports/silver}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_PATH="$REPORT_DIR/build_silver_${SOURCE}_${TIMESTAMP}.log"
LATEST_LOG_PATH="$REPORT_DIR/build_silver_${SOURCE}.log"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-6g}"

if [[ -x ".venv/bin/spark-submit" ]]; then
  SPARK_SUBMIT=".venv/bin/spark-submit"
fi

mkdir -p "$REPORT_DIR"

CMD=(
  "$SPARK_SUBMIT"
  --driver-memory "$SPARK_DRIVER_MEMORY"
  --conf spark.driver.maxResultSize=1g
  --conf spark.sql.shuffle.partitions=4
  src/etl/build_silver.py
  --source "$SOURCE"
)
if [[ -n "$CASES" ]]; then
  CMD+=(--cases "$CASES")
fi

{
  echo "Silver ETL started at $(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "source=$SOURCE"
  echo "cases=${CASES:-ALL}"
  echo "command=${CMD[*]}"
  echo
  "${CMD[@]}"
  echo
  echo "Silver ETL finished at $(date '+%Y-%m-%d %H:%M:%S %z')"
} 2>&1 | tee "$LOG_PATH"

cp "$LOG_PATH" "$LATEST_LOG_PATH"
echo "Wrote $LOG_PATH"
echo "Updated $LATEST_LOG_PATH"
