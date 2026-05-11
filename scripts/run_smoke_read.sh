#!/usr/bin/env bash
set -euo pipefail

RAW_ROOT="${1:-data/raw/train-ticket}"
REPORT_DIR="${2:-reports/smoke}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_PATH="$REPORT_DIR/smoke_read_sources_${TIMESTAMP}.log"
LATEST_LOG_PATH="$REPORT_DIR/smoke_read_sources.log"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"

if [[ -x ".venv/bin/spark-submit" ]]; then
  SPARK_SUBMIT=".venv/bin/spark-submit"
fi

mkdir -p "$REPORT_DIR"

{
  echo "Smoke read started at $(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "raw_root=$RAW_ROOT"
  echo "report_dir=$REPORT_DIR"
  echo
  "$SPARK_SUBMIT" --driver-memory "$SPARK_DRIVER_MEMORY" src/etl/smoke_read_sources.py --raw-root "$RAW_ROOT"
  echo
  echo "Smoke read finished at $(date '+%Y-%m-%d %H:%M:%S %z')"
} 2>&1 | tee "$LOG_PATH"

cp "$LOG_PATH" "$LATEST_LOG_PATH"
echo "Wrote $LOG_PATH"
echo "Updated $LATEST_LOG_PATH"
