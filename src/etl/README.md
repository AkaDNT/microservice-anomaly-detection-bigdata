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
