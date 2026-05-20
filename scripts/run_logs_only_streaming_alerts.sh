#!/usr/bin/env bash
set -euo pipefail

SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"

if [[ -x ".venv/bin/spark-submit" ]]; then
  SPARK_SUBMIT=".venv/bin/spark-submit"
fi

"$SPARK_SUBMIT" \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --conf spark.sql.shuffle.partitions=4 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  src/streaming/logs_only_alerts.py "$@"
