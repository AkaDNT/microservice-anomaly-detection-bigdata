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
