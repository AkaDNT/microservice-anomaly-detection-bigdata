# Local Data Lake

This project uses a local data lake layout that mirrors the layers normally stored on HDFS.

```text
data_lake/
  bronze/
    train-ticket/     raw dataset landing zone or raw path mapping
  silver/
    logs/
    metrics/
    spans/
    trace_edges/
    anomalies/
  gold/
    window_features/
```

For local development, raw files remain in:

```text
data/raw/train-ticket/
```

Spark jobs should treat that folder as the raw bronze source until a full copy or HDFS upload is needed.
