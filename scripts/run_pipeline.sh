#!/usr/bin/env bash
set -euo pipefail

REPORT_DIR="${1:-reports/models}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_PATH="$REPORT_DIR/pipeline_${TIMESTAMP}.log"
LATEST_LOG_PATH="$REPORT_DIR/pipeline.log"

SILVER_SOURCE="${SILVER_SOURCE:-all}"
RUN_SCAN="${RUN_SCAN:-1}"
RUN_SILVER="${RUN_SILVER:-1}"
RUN_GOLD="${RUN_GOLD:-1}"
RUN_VALIDATE="${RUN_VALIDATE:-1}"
RUN_BASELINES="${RUN_BASELINES:-1}"
RUN_FUSION="${RUN_FUSION:-1}"
RUN_DASHBOARD="${RUN_DASHBOARD:-1}"
FUSION_ARGS="${FUSION_ARGS:---algorithms logistic_regression --feature-sets selected_logs_metrics_graph --negative-positive-ratio 50}"

mkdir -p "$REPORT_DIR"

run_step() {
  local name="$1"
  shift
  local start_epoch
  local end_epoch
  start_epoch="$(date +%s)"
  echo
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] START $name"
  "$@"
  end_epoch="$(date +%s)"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] END $name duration_seconds=$((end_epoch - start_epoch))"
}

{
  echo "Train-Ticket pipeline started at $(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "report_dir=$REPORT_DIR"
  echo "silver_source=$SILVER_SOURCE"
  echo "fusion_args=$FUSION_ARGS"

  if [[ "$RUN_SCAN" == "1" ]]; then
    run_step scan_dataset bash scripts/scan_dataset.sh
  fi

  if [[ "$RUN_SILVER" == "1" ]]; then
    run_step build_silver bash scripts/run_silver_etl.sh "$SILVER_SOURCE"
    run_step validate_silver bash scripts/validate_silver.sh
  fi

  if [[ "$RUN_GOLD" == "1" ]]; then
    run_step build_gold bash scripts/run_gold_etl.sh
  fi

  if [[ "$RUN_VALIDATE" == "1" ]]; then
    run_step validate_gold bash scripts/validate_gold.sh
  fi

  if [[ "$RUN_BASELINES" == "1" ]]; then
    run_step train_baselines bash scripts/run_baseline_models.sh
  fi

  if [[ "$RUN_FUSION" == "1" ]]; then
    read -r -a fusion_args <<< "$FUSION_ARGS"
    run_step train_fusion bash scripts/run_fusion_models.sh reports/models "${fusion_args[@]}"
  fi

  if [[ "$RUN_DASHBOARD" == "1" ]]; then
    run_step build_dashboard_assets python src/reports/build_dashboard_assets.py
  fi

  echo
  echo "Train-Ticket pipeline finished at $(date '+%Y-%m-%d %H:%M:%S %z')"
} 2>&1 | tee "$LOG_PATH"

cp "$LOG_PATH" "$LATEST_LOG_PATH"
echo "Wrote $LOG_PATH"
echo "Updated $LATEST_LOG_PATH"
