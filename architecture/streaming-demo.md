# Optional Streaming Demo

This project is primarily a batch Spark data lake pipeline. For a lightweight streaming extension, the repo includes a log replay script that emits structured log rows as JSONL events.

## Local JSONL Replay

```bash
python scripts/demo_streaming_replay.py
```

Output:

```text
reports/dashboard/streaming_replay_sample.jsonl
```

Useful options:

```bash
python scripts/demo_streaming_replay.py --limit 1000 --sleep-seconds 0.05
```

This is enough to demonstrate how raw log events could be replayed into a streaming system before a Kafka/Flink deployment is added.

## Kafka Sketch

If Kafka is available, each JSONL line can be sent to a topic:

```bash
python scripts/demo_streaming_replay.py --output /tmp/train_ticket_logs.jsonl --limit 1000
kafka-console-producer --bootstrap-server localhost:9092 --topic train-ticket-logs < /tmp/train_ticket_logs.jsonl
```

Downstream consumers can compute rolling log counts, error counts, or span-reported counts per service/time window.

## Why This Is Optional

The required project pipeline already covers Spark ETL, silver/gold data lake layers, model training, orchestration, and dashboard assets. Streaming is kept as an extension because the source dataset is static files, not a live telemetry stream.
