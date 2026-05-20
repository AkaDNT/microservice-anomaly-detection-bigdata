# Sprint 6 Summary - Orchestration Va Dashboard

## Trang Thai

Trang thai: Done. Da chay thanh cong pipeline end-to-end trong WSL va tao dashboard assets trong `reports/dashboard`.

Sprint 6 dung lai cac artifact da co tu Sprint 2-5:

- Silver ETL scripts trong `scripts/run_silver_etl.sh`
- Gold ETL script trong `scripts/run_gold_etl.sh`
- Validation scripts trong `scripts/validate_silver.sh`, `scripts/validate_gold.sh`
- Baseline training trong `scripts/run_baseline_models.sh`
- Fusion training trong `scripts/run_fusion_models.sh`

## Artifact Da Tao

Orchestration:

- `scripts/run_pipeline.sh`
- `airflow/dags/train_ticket_pipeline.py`

Dashboard/notebook:

- `src/reports/build_dashboard_assets.py`
- `notebooks/sprint6_dashboard.md`
- `reports/dashboard/model_comparison.csv`
- `reports/dashboard/dashboard_summary.md`
- `reports/dashboard/README.md`

Streaming demo:

- `scripts/produce_logs_to_kafka.py`
- `scripts/run_logs_only_streaming_alerts.sh`
- `src/streaming/logs_only_alerts.py`
- `architecture/kafka-logs-only-demo.html`

## Pipeline Tong

Chay end-to-end bang mot lenh:

```bash
cd /mnt/d/projects/big-data
bash scripts/run_pipeline.sh
```

Mac dinh pipeline se chay:

1. `scan_dataset`
2. `build_silver`
3. `validate_silver`
4. `build_gold`
5. `validate_gold`
6. `train_baselines`
7. `train_fusion`
8. `build_dashboard_assets`

Log tong duoc ghi vao:

```text
reports/models/pipeline_<timestamp>.log
reports/models/pipeline.log
```

Co the tat/bat tung phan bang bien moi truong:

```bash
RUN_SILVER=0 RUN_GOLD=0 RUN_BASELINES=0 bash scripts/run_pipeline.sh
```

Fusion trong pipeline mac dinh chi chay cau hinh tot nhat Sprint 5 de tiet kiem runtime:

```text
--algorithms logistic_regression --feature-sets selected_logs_metrics_graph --negative-positive-ratio 50
```

Neu muon chay full fusion:

```bash
FUSION_ARGS="--negative-positive-ratio 50" bash scripts/run_pipeline.sh
```

## Airflow DAG

DAG:

```text
airflow/dags/train_ticket_pipeline.py
```

Task dependency:

```text
scan_dataset
  -> build_silver_logs
  -> build_silver_metrics
  -> build_silver_traces
  -> build_silver_anomalies
  -> validate_silver
  -> build_gold_features
  -> validate_gold
  -> train_baselines
  -> train_fusion_graph
  -> build_dashboard_assets
```

Silver tasks duoc cau hinh chay song song sau `scan_dataset`.

Bien moi truong quan trong:

- `TRAIN_TICKET_PROJECT_DIR`: mac dinh `/mnt/d/projects/big-data`
- `SPARK_DRIVER_MEMORY`: mac dinh `6g`
- `FUSION_ARGS`: mac dinh chay LR `selected_logs_metrics_graph` ratio `50:1`

## Dashboard Assets

Build dashboard assets:

```bash
python src/reports/build_dashboard_assets.py
```

Output:

```text
reports/dashboard/model_comparison.csv
reports/dashboard/dashboard_summary.md
```

Noi dung dashboard:

- Bang so sanh model theo F1/Precision/Recall.
- Top model theo F1.
- Confusion matrix TP/FP/FN/TN.
- Lich su fusion tu `reports/models/train_fusion_*.log`, tranh mat ket qua khi `fusion_summary.json` bi lan chay sau ghi de.

Notebook demo:

```text
notebooks/sprint6_dashboard.md
```

## Kafka Logs-Only Realtime Demo

Ben canh batch pipeline chinh, Sprint 6 da bo sung demo realtime logs-only bang Kafka va Spark Structured Streaming.

Muc tieu demo:

- Replay structured logs tu dataset Train-Ticket vao Kafka topic `train-ticket-logs`.
- Spark Structured Streaming doc topic nay, parse JSON log events va aggregate thanh window 60s.
- Tao logs-only features: `log_count`, `error_count`, `warn_count`, `info_count`, `unique_event_id_count`, `span_reported_count`, `top_event_frequency`, `template_entropy`.
- Load model artifact `reports/models/artifacts/baseline_logs_only_logistic_regression`.
- Neu anomaly probability vuot threshold thi ghi alert JSON sang Kafka topic `train-ticket-alerts`.

Tai lieu demo HTML:

```text
architecture/kafka-logs-only-demo.html
```

Chay demo bang 3 terminal:

Terminal 1 - Spark streaming alert job:

```bash
cd /mnt/d/projects/big-data
bash scripts/run_logs_only_streaming_alerts.sh \
  --threshold 0.5 \
  --output-mode kafka \
  --starting-offsets latest \
  --checkpoint-dir data_lake/checkpoints/logs_only_alerts_demo
```

Terminal 2 - consume alert:

```bash
cd ~/kafka
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic train-ticket-alerts \
  --from-beginning
```

Terminal 3 - replay logs:

```bash
cd /mnt/d/projects/big-data
python scripts/produce_logs_to_kafka.py \
  --limit 2000 \
  --sleep-seconds 0.001
```

Che do debug console:

```bash
bash scripts/run_logs_only_streaming_alerts.sh \
  --threshold 0.5 \
  --output-mode console \
  --emit-all \
  --starting-offsets latest \
  --checkpoint-dir data_lake/checkpoints/logs_only_alerts_demo_console
```

Luu y quan trong:

- `--output-mode console` chi in ket qua ra Terminal 1, khong ghi sang topic `train-ticket-alerts`.
- Muon Terminal consumer thay alert thi phai dung `--output-mode kafka`.
- Neu can doc lai message cu trong Kafka topic, dung `--starting-offsets earliest` kem checkpoint moi.
- Streaming job dung `top_event_frequency = log_count` lam proxy trong realtime path, vi Spark Structured Streaming khong cho join hai streaming aggregates trong `update` mode.

Ket qua demo da quan sat:

- Spark streaming doc duoc Kafka topic `train-ticket-logs`.
- Console debug da in duoc scored windows voi `alert: anomaly`.
- Vi du probability quan sat duoc: `0.9760` voi threshold `0.01` trong debug run.
- Output alert JSON gom `service_name`, `window_start`, `window_end`, `model`, `probability`, `threshold`, `alert` va cac logs-only features.

## Ket Qua Ky Vong

Ket qua tot nhat hien tai nen hien tren dashboard:

- Model: Logistic Regression `selected_logs_metrics_graph`
- Negative/positive ratio: `50:1`
- Threshold: `0.99`
- Precision: `0.0779`
- Recall: `0.2000`
- F1-score: `0.1121`
- Confusion matrix: TP `6`, FP `71`, FN `24`, TN `133035`

## Kiem Tra Da Lam

- Da syntax-check Python:
  - `src/reports/build_dashboard_assets.py`
  - `airflow/dags/train_ticket_pipeline.py`
- Da chay `bash scripts/run_pipeline.sh` trong WSL luc `2026-05-19 16:03:25 +0700` den `2026-05-19 16:11:47 +0700`.
- Pipeline log:
  - `reports/models/pipeline_20260519_160325.log`
  - `reports/models/pipeline.log`
- Khong thay `Traceback`, `ERROR` hay failed task trong `pipeline.log`.
- Dashboard assets da duoc build vao `reports/dashboard`:
  - `reports/dashboard/dashboard_summary.md`
  - `reports/dashboard/model_comparison_20260519_161147.csv`
  - `reports/dashboard/model_comparison.csv` snapshot truoc do van duoc giu lai.

Runtime theo task:

| Task | Duration |
|---|---:|
| `scan_dataset` | 9s |
| `build_silver` | 209s |
| `validate_silver` | 45s |
| `build_gold` | 77s |
| `validate_gold` | 30s |
| `train_baselines` | 79s |
| `train_fusion` | 53s |
| `build_dashboard_assets` | 0s |

Ket qua validation/model chinh:

- Gold `window_features`: 401,806 rows, 10 cases, 40 columns.
- Label 0: 401,615.
- Label 1: 191.
- Fusion model trong pipeline: Logistic Regression `selected_logs_metrics_graph`, negative ratio `50:1`.
- Best threshold: `0.99`.
- Precision: `0.0779`.
- Recall: `0.2000`.
- F1-score: `0.1121`.
- Confusion matrix: TP `6`, FP `71`, FN `24`, TN `133035`.

## Definition Of Done Sprint 6

| Tieu chi | Trang thai |
|---|---|
| Co mot lenh chay pipeline end-to-end | Done qua `scripts/run_pipeline.sh` |
| Co Airflow DAG toi thieu | Done qua `airflow/dags/train_ticket_pipeline.py` |
| Co dependency silver -> gold -> training -> dashboard | Done |
| Co log runtime tung buoc | Done trong `run_pipeline.sh` |
| Co dashboard/notebook demo | Done qua `notebooks/sprint6_dashboard.md` |
| Co dashboard assets generator | Done qua `src/reports/build_dashboard_assets.py` |
| Co dashboard snapshot trong `reports/dashboard` | Done |
| Da chay end-to-end trong WSL | Done qua `reports/models/pipeline.log` |
| Co demo Kafka logs-only realtime alert | Done qua `scripts/produce_logs_to_kafka.py`, `src/streaming/logs_only_alerts.py`, `architecture/kafka-logs-only-demo.html` |

Ket luan: Sprint 6 da Done. Co the dung `reports/models/pipeline.log`, `reports/dashboard/dashboard_summary.md` va `architecture/kafka-logs-only-demo.html` lam bang chung cho bao cao/demo.

## Hardening Sau Sprint 6

Da bo sung cac muc ky thuat de project chac hon:

| Hang muc | Artifact |
|---|---|
| Unit test tu dong | `tests/test_dashboard_assets.py` |
| Airflow retry/alert/timeout | `airflow/dags/train_ticket_pipeline.py` |
| Model artifact saving | `src/models/train_fusion.py`, `--model-output-dir reports/models/artifacts` |
| Schema validation | `src/etl/validate_schemas.py`, `scripts/validate_schemas.sh` |
| CI toi thieu | `.github/workflows/ci.yml` |
| Streaming replay demo | `scripts/demo_streaming_replay.py`, `architecture/streaming-demo.md` |
| Kafka logs-only realtime alert demo | `scripts/produce_logs_to_kafka.py`, `src/streaming/logs_only_alerts.py`, `scripts/run_logs_only_streaming_alerts.sh`, `architecture/kafka-logs-only-demo.html` |

Luu y: sau khi them `validate_schemas` va model artifact saving, nen chay lai `bash scripts/run_pipeline.sh` mot lan trong WSL de tao log moi va model artifact folder.

Neu `reports/dashboard/model_comparison.csv` dang bi OS lock, dashboard generator se ghi fallback thanh `reports/dashboard/model_comparison_<timestamp>.csv`.

## Hardening Validation Run

Da chay lai pipeline sau khi bo sung schema validation va model artifact saving.

Log moi nhat:

- `reports/models/pipeline_20260519_220649.log`
- `reports/models/pipeline.log`

Ket qua:

- `validate_schemas` da pass:
  - `silver.logs`: 1,148,240 rows, 13 columns.
  - `silver.metrics`: 12,684,274 rows, 13 columns.
  - `silver.spans`: 219,252 rows, 18 columns.
  - `silver.trace_edges`: 2,919,729 rows, 12 columns.
  - `silver.anomalies`: 103 rows, 9 columns.
  - `gold.window_features`: 401,806 rows, 40 columns.
- Model artifact da duoc luu:
  - `reports/models/artifacts/fusion_selected_logs_metrics_graph_logistic_regression`
- Dashboard assets da duoc ghi:
  - `reports/dashboard/model_comparison.csv`
  - `reports/dashboard/dashboard_summary.md`
- Fusion best giu nguyen:
  - Model: LR `selected_logs_metrics_graph`.
  - Negative ratio: `50:1`.
  - Threshold: `0.99`.
  - F1-score: `0.1121`.

Canh bao `Failed to load implementation from dev.ludovic.netlib.blas...` chi la Spark/BLAS native warning, khong lam task fail.

## Noi Dung Nen Ghi Vao Bao Cao

- Project chinh la batch Big Data pipeline tren Spark theo kien truc bronze/silver/gold.
- Sprint 6 tu dong hoa pipeline bang `scripts/run_pipeline.sh` va Airflow DAG, giup chay lai silver -> gold -> validation -> training -> dashboard bang mot luong thong nhat.
- Dashboard assets trong `reports/dashboard` dung de bao cao ket qua model, confusion matrix va best threshold.
- Model artifact saving cho phep tach training va inference: model train batch duoc luu trong `reports/models/artifacts/`, sau do co the load lai cho realtime demo.
- Kafka logs-only demo la phan mo rong realtime: static logs duoc replay thanh stream, Spark Structured Streaming tao feature theo window va sinh anomaly alert.
- Alert output co dang JSON, gom service, time window, probability, threshold, label alert va cac feature logs-only de giai thich.
- Gioi han can neu ro: demo realtime moi la logs-only, chua phai fusion realtime logs+metrics+traces; `top_event_frequency` trong realtime path dung proxy `log_count` de tranh stream-stream aggregate join.
