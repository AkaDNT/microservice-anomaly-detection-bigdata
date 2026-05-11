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
