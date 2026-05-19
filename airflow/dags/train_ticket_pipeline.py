from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = os.environ.get("TRAIN_TICKET_PROJECT_DIR", "/mnt/d/projects/big-data")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "6g")
FUSION_ARGS = os.environ.get(
    "FUSION_ARGS",
    "--algorithms logistic_regression --feature-sets selected_logs_metrics_graph --negative-positive-ratio 50",
)


default_args = {
    "owner": "big-data-project",
    "depends_on_past": False,
    "retries": 0,
}


def project_bash(command: str) -> str:
    return f"cd {PROJECT_DIR} && export SPARK_DRIVER_MEMORY={SPARK_DRIVER_MEMORY} && {command}"


with DAG(
    dag_id="train_ticket_anomaly_pipeline",
    description="Train-Ticket bronze/silver/gold/model/dashboard pipeline for anomaly detection.",
    default_args=default_args,
    start_date=datetime(2026, 5, 19),
    schedule=None,
    catchup=False,
    tags=["big-data", "train-ticket", "anomaly-detection"],
) as dag:
    scan_dataset = BashOperator(
        task_id="scan_dataset",
        bash_command=project_bash("bash scripts/scan_dataset.sh"),
    )

    build_silver_logs = BashOperator(
        task_id="build_silver_logs",
        bash_command=project_bash("bash scripts/run_silver_etl.sh logs"),
    )

    build_silver_metrics = BashOperator(
        task_id="build_silver_metrics",
        bash_command=project_bash("bash scripts/run_silver_etl.sh metrics"),
    )

    build_silver_traces = BashOperator(
        task_id="build_silver_traces",
        bash_command=project_bash("bash scripts/run_silver_etl.sh traces"),
    )

    build_silver_anomalies = BashOperator(
        task_id="build_silver_anomalies",
        bash_command=project_bash("bash scripts/run_silver_etl.sh anomalies"),
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=project_bash("bash scripts/validate_silver.sh"),
    )

    build_gold_features = BashOperator(
        task_id="build_gold_features",
        bash_command=project_bash("bash scripts/run_gold_etl.sh"),
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=project_bash("bash scripts/validate_gold.sh"),
    )

    train_baselines = BashOperator(
        task_id="train_baselines",
        bash_command=project_bash("bash scripts/run_baseline_models.sh"),
    )

    train_fusion_graph = BashOperator(
        task_id="train_fusion_graph",
        bash_command=project_bash(f"bash scripts/run_fusion_models.sh reports/models {FUSION_ARGS}"),
    )

    build_dashboard_assets = BashOperator(
        task_id="build_dashboard_assets",
        bash_command=project_bash("python src/reports/build_dashboard_assets.py"),
    )

    silver_tasks = [build_silver_logs, build_silver_metrics, build_silver_traces, build_silver_anomalies]
    scan_dataset >> silver_tasks
    for silver_task in silver_tasks:
        silver_task >> validate_silver

    validate_silver >> build_gold_features >> validate_gold

    for model_task in [train_baselines, train_fusion_graph]:
        validate_gold >> model_task >> build_dashboard_assets
