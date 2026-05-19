# ETL Jobs

This folder contains Apache Spark jobs for moving Train-Ticket telemetry from raw files into bronze, silver, and gold layers.

Current Sprint 1 job:

```powershell
spark-submit src/etl/smoke_read_sources.py --raw-root data/raw/train-ticket
```

The smoke job validates that Spark can read:

- structured log CSV files,
- Prometheus-style monitoring JSON files,
- Jaeger-style trace JSON files.

## Sprint 2 Silver ETL

Build one silver source:

```bash
bash scripts/run_silver_etl.sh logs
bash scripts/run_silver_etl.sh metrics
bash scripts/run_silver_etl.sh traces
bash scripts/run_silver_etl.sh anomalies
```

Build all silver sources:

```bash
bash scripts/run_silver_etl.sh all
```

Debug on selected cases:

```bash
bash scripts/run_silver_etl.sh all "case_07_order_mongodb_4_2_2_20220712,case_10_order_springdata_mongodb_2_0_0_20220711"
```

Validate silver outputs:

```bash
bash scripts/validate_silver.sh
```

Expected silver outputs:

```text
data_lake/silver/logs/
data_lake/silver/metrics/
data_lake/silver/spans/
data_lake/silver/trace_edges/
data_lake/silver/anomalies/
```

## Sprint 3 Gold ETL

Build 60-second window features:

```bash
bash scripts/run_gold_etl.sh
```

Build with a different window size:

```bash
bash scripts/run_gold_etl.sh 30
bash scripts/run_gold_etl.sh 120
```

Validate gold outputs:

```bash
bash scripts/validate_gold.sh
```

Expected gold output:

```text
data_lake/gold/window_features/
```

Gold rows use this key:

```text
case_id, service_name, window_start, window_end
```

The current gold job creates log, metric, trace, graph, and relaxed anomaly label features.
Anomaly labels use a default 120-second buffer around each 60-second window and can use services inferred from anomaly text or services found on the same trace id.

## Sprint 4 Baseline Models

Train single-source baselines from gold features:

```bash
bash scripts/run_baseline_models.sh
```

Expected metric outputs:

```text
reports/metrics/baseline_logs.json
reports/metrics/baseline_metrics.json
reports/metrics/baseline_traces.json
reports/metrics/baseline_summary.json
```

The baseline job trains Spark ML Logistic Regression models for logs-only, metrics-only, and traces-only feature groups.
It also tunes the classification threshold from 0.01 to 0.99 and stores the best F1 threshold in each JSON report.

Optional Random Forest baselines:

```bash
.venv/bin/spark-submit src/models/train_baselines.py --include-random-forest
```

## Sprint 5 Fusion Models

Train fusion and graph-enhanced models from gold features:

```bash
bash scripts/run_fusion_models.sh
```

The default Sprint 5 run uses selected logs+metrics feature sets plus lightweight trace/graph variants, with train negative downsampling at `50:1`.

To tune the negative sampling ratio:

```bash
bash scripts/run_fusion_models.sh --negative-positive-ratio 20
bash scripts/run_fusion_models.sh reports/models --negative-positive-ratio 20
```

Expected metric outputs:

```text
reports/metrics/fusion_logs_metrics_random_forest.json
reports/metrics/fusion_selected_logs_metrics_random_forest.json
reports/metrics/fusion_selected_logs_metrics_trace_latency_random_forest.json
reports/metrics/fusion_selected_logs_metrics_graph_random_forest.json
reports/metrics/fusion_logs_metrics_logistic_regression.json
reports/metrics/fusion_selected_logs_metrics_logistic_regression.json
reports/metrics/fusion_selected_logs_metrics_trace_latency_logistic_regression.json
reports/metrics/fusion_selected_logs_metrics_graph_logistic_regression.json
reports/metrics/fusion_summary.json
```

The original full trace/graph feature sets are still available:

```bash
bash scripts/run_fusion_models.sh reports/models --feature-sets logs_metrics_traces,logs_metrics_traces_graph
```

## Sprint 6 Orchestration And Dashboard

Run the lightweight end-to-end pipeline:

```bash
bash scripts/run_pipeline.sh
```

The pipeline logs each step with start/end timestamps and writes:

```text
reports/models/pipeline_<timestamp>.log
reports/models/pipeline.log
```

Build dashboard-ready assets from existing metrics and fusion logs:

```bash
python src/reports/build_dashboard_assets.py
```

Expected dashboard outputs:

```text
reports/dashboard/model_comparison.csv
reports/dashboard/dashboard_summary.md
```

Airflow DAG:

```text
airflow/dags/train_ticket_pipeline.py
```
