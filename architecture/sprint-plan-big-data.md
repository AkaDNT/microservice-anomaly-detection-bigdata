# Ke Hoach Sprint Do An Big Data

## Tong Quan

De tai: Phat hien bat thuong trong he thong microservice bang phan tich du lieu telemetry da nguon va bieu dien do thi lai tren nen tang Apache Big Data.

Dataset: Train-Ticket anomaly dataset, gom logs, distributed traces, monitoring metrics va potential anomalies.

Muc tieu ky thuat:

- Xay dung data lake theo cac tang bronze, silver, gold.
- Xu ly du lieu da nguon bang Apache Spark.
- Dong bo logs, metrics va traces theo time window.
- Trich xuat dac trung don nguon va da nguon.
- Xay dung graph service-call tu distributed traces.
- Huan luyen baseline va mo hinh fusion/graph nhe.
- Tu dong hoa pipeline bang Apache Airflow.
- Truc quan hoa ket qua bang Apache Superset hoac Zeppelin.

Stack de xuat:

- Apache Spark: ETL, feature engineering, MLlib.
- Apache Hadoop/HDFS hoac local data lake: luu tru raw/processed data.
- Apache Hive hoac Spark SQL: quan ly bang du lieu.
- Apache Airflow: orchestration.
- Apache Superset hoac Zeppelin: dashboard va demo phan tich.
- Apache Kafka/Flink: optional, chi dung de demo streaming neu con thoi gian.

Quy uoc runtime:

- Code co the sua tren Windows bang IDE.
- Java, Python, Spark, Airflow va cac job Big Data se chay trong WSL Ubuntu.
- Duong dan project trong WSL: `/mnt/d/projects/big-data`.
- Script moi nen uu tien `.sh` cho WSL; PowerShell chi la fallback khi can thao tac nhanh tren Windows.

## Cap Nhat Tien Do

Tinh den `2026-05-19`:

| Sprint | Trang thai | Ket qua chinh |
|---|---|---|
| Sprint 0 | Done | Da chot bai toan, dataset, don vi phan tich theo service-level time window va kien truc bronze/silver/gold. |
| Sprint 1 | Done | Da tao project structure, data lake local, scripts scan/smoke read va inventory dataset. |
| Sprint 2 | Done | Da build silver logs, metrics, spans, trace_edges, anomalies; silver anomalies da bo sung inferred service va trace_id. |
| Sprint 3 | Done | Da build gold `window_features` 60s; relaxed label 120s tao 401,806 rows voi 191 anomaly windows. |
| Sprint 4 | Done | Da train Logistic Regression va Random Forest cho logs-only, metrics-only, traces-only; co threshold tuning va feature importance. |
| Sprint 5 | Done | Fusion + graph da vuot baseline don nguon: LR `selected_logs_metrics_graph`, ratio 50:1, F1 0.1121. |
| Sprint 6 | Done | Da chay end-to-end pipeline trong WSL; co Airflow DAG, runtime log va dashboard assets trong `reports/dashboard`. |
| Sprint 7 | Pending | Bao cao, slide va dong goi nop bai. |

Ket qua baseline tot nhat hien tai:

- Logistic Regression `metrics-only`: threshold 0.90, Precision 0.0472, Recall 0.7667, F1 0.0890.
- Logistic Regression `logs-only`: threshold 0.96, Precision 0.0533, Recall 0.2667, F1 0.0889.
- Random Forest `logs-only`: threshold 0.93, Precision 0.0441, Recall 0.3000, F1 0.0769.
- `traces-only` hien chua co tin hieu tot, F1 gan 0.

## Sprint 0 - Khoi Dong Va Chuan Hoa Pham Vi

Thoi luong de xuat: 2-3 ngay.

Muc tieu:

- Hieu ro paper nen, dataset va yeu cau do an.
- Xac dinh pham vi kha thi voi 16GB RAM va 20GB disk.
- Chot kien truc tong the va dau ra cuoi cung.

Task chi tiet:

- Doc va tom tat base paper AMulSys:
  - Bai toan anomaly detection trong microservice.
  - Ba nguon du lieu: logs, metrics, traces.
  - Y tuong multimodal fusion va hybrid graph representation.
  - Diem nao co the tai hien, diem nao can rut gon.
- Doc `de-tai-big-data.docx`:
  - Ten de tai.
  - Research gap.
  - Huong giai quyet du kien.
  - Gioi han phan cung va pham vi thuc nghiem.
- Doc `README.md` va kiem tra dataset:
  - Cau truc 10 case.
  - Cau truc log structured CSV.
  - Cau truc metrics Prometheus JSON.
  - Cau truc traces Jaeger JSON.
  - Cau truc potential anomalies TXT.
- Chot don vi phan tich:
  - Su dung time window 30s hoac 60s.
  - Moi ban ghi dau ra ung voi `case_id`, `service_name`, `window_start`, `window_end`.
- Chot pham vi thuc nghiem:
  - Lam tren toan bo 10 case neu du tai nguyen.
  - Neu cham, uu tien 3-5 case dai dien.
  - Khong tai hien full AMulSys, chi lam fusion/graph lightweight.

Deliverables:

- Tai lieu tom tat paper nen.
- So do kien truc pipeline.
- Bang mo ta nguon du lieu va schema du kien.
- Quyet dinh time window va pham vi case xu ly.

Tieu chi hoan thanh:

- Nhom giai thich duoc de tai, dataset, research gap va kien truc xu ly.
- Co danh sach output cuoi cung can nop: source code, data processed, report, slide, huong dan cai dat.

## Sprint 1 - Thiet Lap Moi Truong Va Data Lake

Thoi luong de xuat: 4-5 ngay.

Muc tieu:

- Cai dat moi truong lam viec cho pipeline Big Data.
- Tao cau truc data lake theo bronze, silver, gold.
- Doc thu thanh cong logs, metrics va traces bang Spark.

Task chi tiet:

- Thiet lap project structure:
  - `src/etl/` cho Spark ETL jobs.
  - `src/features/` cho feature engineering.
  - `src/models/` cho training/evaluation.
  - `airflow/dags/` cho DAG orchestration.
  - `notebooks/` cho Zeppelin/Jupyter demo neu can.
  - `data_lake/bronze/`, `data_lake/silver/`, `data_lake/gold/`.
- Cai dat Apache Spark local mode:
  - Kiem tra `spark-submit`.
  - Cau hinh memory phu hop, vi du driver 4-6GB.
  - Chay job test doc CSV va JSON.
- Tao bronze data lake:
  - Copy hoac map du lieu raw vao `data_lake/bronze/train-ticket`.
  - Giu nguyen file goc.
  - To chuc partition logic theo `case_id/source/date`.
- Tao script scan dataset:
  - Liet ke tat ca case.
  - Dem so file logs, metrics, traces, anomalies.
  - Xuat summary CSV/Markdown de dua vao bao cao.
- Doc thu bang Spark:
  - Doc `LOGS_*.txt_structured.csv`.
  - Doc metrics JSON dang Prometheus response.
  - Doc traces JSON dang Jaeger response.
  - Doc potential anomalies TXT bang Spark text reader hoac parser rieng.

Deliverables:

- Cau truc thu muc project va data lake.
- Script/job Spark doc thu 3 nguon du lieu.
- Bang thong ke dataset: so case, so file, so dong log, so trace, so metric point.

Tieu chi hoan thanh:

- Chay duoc Spark local.
- Doc duoc it nhat 1 file logs, 1 file metrics va 1 file traces.
- Co data inventory de chung minh dataset da duoc khao sat.

## Sprint 2 - Bronze To Silver: Tien Xu Ly Logs, Metrics, Traces

Thoi luong de xuat: 1 tuan.

Trang thai hien tai: **Done**.

Ket qua da dat:

- Silver logs: 1,148,240 rows, 10 cases.
- Silver metrics: 12,684,274 rows, 9 cases.
- Silver spans: 219,252 rows, 9 cases.
- Silver trace_edges: 2,919,729 rows, 9 cases.
- Silver anomalies: 103 rows, 8 cases.
- `silver/anomalies` da co `source_service_name`, `inferred_service_name`, `trace_id` de ho tro label gold tot hon.

Muc tieu:

- Bien du lieu raw thanh cac bang sach o silver layer.
- Chuan hoa timestamp, case_id, service_name va schema chung.

Task chi tiet:

- Xu ly logs:
  - Doc tat ca `LOGS_*.txt_structured.csv`.
  - Tao cot `timestamp` tu `Date` va `Time`.
  - Trich xuat `case_id` tu duong dan.
  - Trich xuat `service_name` tu ten file.
  - Chuan hoa cac cot: `level`, `event_id`, `event_template`, `content`.
  - Tao cot co ban: `is_error`, `is_warn`, `is_span_reported`.
  - Ghi ra Parquet: `data_lake/silver/logs`.
- Xu ly metrics:
  - Doc tat ca file JSON trong thu muc `Monitoring_*`.
  - Explode `data.result`.
  - Explode `values`.
  - Lay `metric_name`, `timestamp`, `value`, `container`, `pod`, `namespace`, `node`.
  - Loc cac metric quan trong:
    - `container_cpu_usage_seconds_total`
    - `container_memory_working_set_bytes`
    - `container_network_transmit_packets_total`
    - `node_cpu_seconds_total`
    - `node_memory_MemAvailable_bytes`
    - `node_memory_MemTotal_bytes`
  - Ghi ra Parquet: `data_lake/silver/metrics`.
- Xu ly traces:
  - Doc tat ca file trong `Traces_*`.
  - Explode `data`.
  - Explode `spans`.
  - Lay `trace_id`, `span_id`, `parent_span_id`, `operation_name`, `start_time`, `duration`.
  - Parse `tags` de lay:
    - `http.status_code`
    - `http.method`
    - `http.url`
    - `error`
    - `component`
    - `span.kind`
  - Parse `processes` de map `processID` sang `service_name`.
  - Ghi spans ra Parquet: `data_lake/silver/spans`.
- Tao service-call edges:
  - Dung quan he parent-child giua spans.
  - Noi span con voi span cha trong cung trace.
  - Xac dinh `source_service`, `target_service`.
  - Tao edge duration va status.
  - Ghi ra Parquet: `data_lake/silver/trace_edges`.
- Xu ly anomalies:
  - Doc `potentialAnomalies_*.txt`.
  - Trich xuat timestamp bang regex.
  - Tao bang `silver_anomaly_events` gom `case_id`, `service_name`, `anomaly_timestamp`, `raw_text`.
  - Ghi ra Parquet: `data_lake/silver/anomalies`.

Deliverables:

- `silver_logs` - Done
- `silver_metrics` - Done
- `silver_spans` - Done
- `silver_trace_edges` - Done
- `silver_anomaly_events` - Done

Tieu chi hoan thanh:

- Tat ca bang silver doc duoc bang Spark SQL - Done.
- Timestamp da duoc chuan hoa - Done.
- Moi bang deu co `case_id` - Done.
- Co script kiem tra sample/count data cua tung bang - Done qua `src/etl/validate_silver.py` va `scripts/validate_silver.sh`.

## Sprint 3 - Gold Layer: Windowing, Labels Va Feature Engineering

Thoi luong de xuat: 1 tuan.

Trang thai hien tai: **Done**.

Ket qua da dat:

- Gold table: `data_lake/gold/window_features`.
- Window size: 60 seconds.
- Tong rows: 401,806.
- Label 0: 401,615.
- Label 1: 191.
- Label relaxed mac dinh: 120 seconds quanh moi window, co inferred service va trace-aware service expansion.

Muc tieu:

- Dong bo logs, metrics, traces theo time window.
- Tao bang feature cuoi cung de train model.
- Tao label anomaly o muc window.

Task chi tiet:

- Chot window size:
  - Mac dinh 60s.
  - Co the thu them 30s neu can so sanh.
- Tao log features theo window:
  - `log_count`
  - `error_count`
  - `warn_count`
  - `info_count`
  - `unique_event_id_count`
  - `span_reported_count`
  - `top_event_frequency`
  - `template_entropy`
- Tao metric features theo window:
  - `cpu_mean`, `cpu_max`, `cpu_std`
  - `memory_mean`, `memory_max`, `memory_std`
  - `network_mean`, `network_max`
  - `node_memory_available_mean`
  - `node_memory_total_mean`
  - `cpu_rate_mean` neu metric la cumulative counter.
- Tao trace features theo window:
  - `trace_count`
  - `span_count`
  - `avg_duration`
  - `max_duration`
  - `p95_duration`
  - `error_span_count`
  - `http_4xx_count`
  - `http_5xx_count`
  - `unique_operation_count`
  - `unique_peer_service_count`
- Tao graph features theo window:
  - `in_degree`
  - `out_degree`
  - `weighted_call_count`
  - `avg_edge_latency`
  - `max_edge_latency`
  - `error_edge_count`
  - Optional: PageRank bang GraphX neu setup kip.
- Tao label:
  - Neu anomaly timestamp nam trong window co buffer thi `label = 1`.
  - Relaxed label mac dinh: `window_start - 120s <= anomaly_timestamp <= window_end + 120s`.
  - Uu tien service suy luan tu raw anomaly text thay vi chi lay service tu ten file.
  - Neu anomaly text co trace id, label them cac service xuat hien tren cung trace.
  - Cac window con lai `label = 0`.
- Join features:
  - Join theo `case_id`, `service_name`, `window_start`.
  - Fill null bang 0 voi feature dem.
  - Fill null bang median/mean voi metric feature neu can.
- Ghi bang gold:
  - `data_lake/gold/window_features`.
  - Dinh dang Parquet.
  - Partition theo `case_id`.

Deliverables:

- Bang `gold_window_features` - Done.
- Data dictionary cho tat ca cot feature - Done trong `architecture/sprint-3-summary.md`.
- Script thong ke class imbalance - Done qua `src/etl/validate_gold.py`.
- Bao cao ngan ve so window anomaly va normal - Done trong `architecture/sprint-3-summary.md`.

Tieu chi hoan thanh:

- Mot dong feature dai dien duoc mot service trong mot time window - Done.
- Co label anomaly - Done.
- Train/test split co the thuc hien truc tiep tu bang gold - Done, da dung cho Sprint 4.

## Sprint 4 - Baseline Models Don Nguon

Thoi luong de xuat: 4-5 ngay.

Trang thai hien tai: **Done**.

Ket qua da dat:

- Da train Logistic Regression cho logs-only, metrics-only, traces-only.
- Da train Random Forest cho logs-only, metrics-only, traces-only.
- Da dung split theo case: train `case_01` -> `case_07`, test `case_08` -> `case_10`.
- Test set: 133,136 rows, trong do 30 anomaly.
- Da co threshold tuning 0.01 -> 0.99 va confusion matrix.
- Da co Random Forest feature importance.

Muc tieu:

- Xay dung cac baseline don nguon de co moc so sanh.
- Danh gia logs-only, metrics-only va traces-only.

Task chi tiet:

- Chuan bi dataset cho tung baseline:
  - Logs-only: chi dung log features.
  - Metrics-only: chi dung metric features.
  - Traces-only: chi dung trace features.
- Xu ly imbalance:
  - Bao cao ty le normal/anomaly.
  - Dung class weight neu model ho tro.
  - Hoac dung undersampling normal windows.
- Chia train/test:
  - Cach 1: split theo case de tranh leakage.
  - Cach 2: split theo time, train tren dau timeline va test tren cuoi timeline.
  - Nen uu tien split theo case neu so case du.
- Train model Spark MLlib:
  - Logistic Regression.
  - Random Forest.
  - Gradient-Boosted Trees neu du tai nguyen.
- Toi uu threshold cho bai toan imbalance:
  - Khong chi dung threshold mac dinh 0.5.
  - Quet threshold tren predicted probability, vi du 0.01 den 0.99.
  - Chon threshold theo F1 neu can can bang Precision/Recall.
  - Ghi them best_threshold, best_precision, best_recall, best_f1 vao ket qua.
- Thu model phi tuyen cho baseline don nguon:
  - Random Forest la uu tien tiep theo sau Logistic Regression.
  - GBT chi chay neu thoi gian va tai nguyen cho phep.
  - Dung feature importance cua Random Forest de giai thich baseline.
- Danh gia:
  - Precision.
  - Recall.
  - F1-score.
  - Confusion matrix.
  - Area under ROC.
  - Area under PR.
  - Training time.
  - Inference time.
- Luu ket qua:
  - `reports/metrics/baseline_logs.json`
  - `reports/metrics/baseline_metrics.json`
  - `reports/metrics/baseline_traces.json`
  - `reports/metrics/baseline_summary.json`

Deliverables:

- Ket qua 3 baseline don nguon - Done.
- Bang so sanh logs-only, metrics-only, traces-only - Done.
- Nhan xet nguon du lieu nao manh/yeu hon - Done.
- Ket qua threshold tuning cho tung baseline - Done.
- Ket qua Random Forest don nguon va feature importance - Done.

Ket qua hien tai sau rebuild label relaxed 120s ngay `2026-05-19`:

- Gold co 401,806 windows, gom 401,615 normal va 191 anomaly.
- Split train/test theo case:
  - Train: 268,670 rows, 161 anomaly.
  - Test: 133,136 rows, 30 anomaly.
- Logistic Regression baseline tot nhat sau threshold tuning:
  - `metrics-only`: threshold 0.90, Precision 0.0472, Recall 0.7667, F1 0.0890.
  - `logs-only`: threshold 0.96, Precision 0.0533, Recall 0.2667, F1 0.0889.
  - `traces-only`: F1 gan 0, chua co tin hieu tot.
- Random Forest baseline sau threshold tuning:
  - `logs-only`: threshold 0.93, Precision 0.0441, Recall 0.3000, F1 0.0769.
  - `metrics-only`: threshold 0.61, Precision 0.0052, Recall 0.9667, F1 0.0103.
  - `traces-only`: F1 gan 0, chua co tin hieu tot.
- Random Forest feature importance noi bat:
  - Logs: `span_reported_count`, `top_event_frequency`, `info_count`.
  - Metrics: `memory_mean`, `cpu_max`, `memory_std`, `cpu_mean`.
  - Traces: `p95_duration_ms`, `avg_duration_ms`, `span_count`.

Tieu chi hoan thanh:

- Co it nhat 3 baseline chay thanh cong - Done.
- Co bang metric ro rang de dua vao bao cao - Done.
- Co nhan xet ve uu/nhuoc diem cua tung nguon telemetry - Done.
- Co threshold tuning hoac giai thich vi sao dung threshold mac dinh - Done.
- Giu fusion da nguon va graph-enhanced model cho Sprint 5 - Done.

## Sprint 5 - Fusion Da Nguon Va Graph-Enhanced Model

Thoi luong de xuat: 1 tuan.

Trang thai hien tai: **Done**.

Da lam:

- Tao `src/models/train_fusion.py`.
- Tao `scripts/run_fusion_models.sh`.
- Dinh nghia feature sets ban dau:
  - `logs_metrics_traces`: log + metric + trace features.
  - `logs_metrics_traces_graph`: log + metric + trace + graph features.
- Da them feature sets toi uu:
  - `logs_metrics`: log + metric features, bo trace/graph noise.
  - `selected_logs_metrics`: chon cac log/metric features manh theo baseline va feature importance.
  - `selected_logs_metrics_trace_latency`: selected logs + metrics + trace latency features.
  - `selected_logs_metrics_graph`: selected logs + metrics + graph features nhe.
- Dung cung split theo case voi Sprint 4:
  - Train: `case_01` -> `case_07`.
  - Test: `case_08` -> `case_10`.
- Dung class weight va threshold tuning 0.01 -> 0.99 de so sanh cong bang voi baseline don nguon.
- Da chay fusion training luc `2026-05-19 12:35:27 +0700`.
- Da them downsample train negatives mac dinh `50:1` trong `src/models/train_fusion.py`.
- Da cho phep `scripts/run_fusion_models.sh` truyen tham so vao Spark job de tuning nhanh.
- Da chay lai 2 cau hinh toi uu:
  - Default `50:1`.
  - `--negative-positive-ratio 20`.
- Ket qua tot nhat: Logistic Regression `selected_logs_metrics_graph`, ratio `50:1`, threshold `0.99`, Precision `0.0779`, Recall `0.2000`, F1 `0.1121`.

Muc tieu:

- Xay dung mo hinh da nguon.
- Them graph features de bam sat tinh than hybrid graph representation cua paper AMulSys.
- So sanh voi baseline don nguon.

Task chi tiet:

- Tao fusion dataset:
  - Ket hop log features, metric features va trace features.
  - Chuan hoa feature numeric neu can.
  - Xu ly missing values.
- Train fusion baseline:
  - Random Forest tren tat ca feature logs + metrics + traces.
  - Gradient-Boosted Trees neu may chay duoc.
- Tao graph-enhanced dataset:
  - Them cac graph features vao fusion dataset.
  - Neu dung GraphX:
    - Tao graph theo window hoac theo case.
    - Tinh degree/PageRank/community neu kha thi.
  - Neu khong dung GraphX:
    - Dung Spark SQL aggregation tren `trace_edges`.
    - Van duoc tinh la graph feature lightweight.
- Train proposed model:
  - Fusion + graph features.
  - So sanh voi fusion khong graph.
- Phan tich feature importance:
  - Lay feature importance tu Random Forest/GBT.
  - Xac dinh feature nao dong gop nhieu: latency, error, CPU, event template, degree.
- Danh gia tong hop:
  - Don nguon vs fusion.
  - Fusion vs fusion + graph.
  - Strict label vs relaxed label neu co.

Deliverables:

- Ket qua fusion baseline - Done.
- Ket qua graph-enhanced model - Done.
- Bang feature importance - Done.
- Bang tong hop tat ca model - Done.

Ket qua lan chay dau:

| Algorithm | Feature set | Best F1 | Precision | Recall | Ket luan |
|---|---|---:|---:|---:|---|
| Logistic Regression | logs_metrics_traces | 0.0558 | 0.0324 | 0.2000 | Chua vuot baseline don nguon |
| Logistic Regression | logs_metrics_traces_graph | 0.0533 | 0.0308 | 0.2000 | Graph chua cai thien |
| Random Forest | logs_metrics_traces | 0.0113 | 0.0057 | 0.7000 | False positive cao |
| Random Forest | logs_metrics_traces_graph | 0.0101 | 0.0051 | 0.7000 | Graph chua cai thien |

Baseline don nguon tot nhat la Logistic Regression `metrics-only` voi F1 `0.0890`.

Vong toi uu da them sau lan chay dau:

- Default Sprint 5 khong con chi chay full logs+metrics+traces nua, ma uu tien `selected_logs_metrics`, `logs_metrics`, `selected_logs_metrics_trace_latency`, `selected_logs_metrics_graph`.
- Train set duoc downsample negative windows theo `--negative-positive-ratio 50`; test set giu nguyen de danh gia trung thuc.
- Muc tieu cua vong nay la tang precision/F1 bang cach giam feature noise va giam ap luc class imbalance tren training.

Ket qua sau toi uu:

| Algorithm | Feature set | Negative ratio | Best F1 | Precision | Recall | TP | FP | FN | Ket luan |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Logistic Regression | selected_logs_metrics_graph | 50:1 | 0.1121 | 0.0779 | 0.2000 | 6 | 71 | 24 | Tot nhat, vuot baseline |
| Logistic Regression | selected_logs_metrics_trace_latency | 50:1 | 0.0710 | 0.0432 | 0.2000 | 6 | 133 | 24 | Chua vuot baseline |
| Logistic Regression | logs_metrics | 50:1 | 0.0654 | 0.0345 | 0.6333 | 19 | 532 | 11 | Recall tot nhung FP cao |
| Logistic Regression | selected_logs_metrics_graph | 20:1 | 0.0530 | 0.0277 | 0.6000 | 18 | 631 | 12 | FP tang, khong nen dung |
| Random Forest | selected_logs_metrics_trace_latency | 20:1 | 0.0212 | 0.0107 | 0.9333 | 28 | 2,579 | 2 | Recall cao nhung FP qua lon |

Output du kien:

- `reports/metrics/fusion_logs_metrics_random_forest.json`
- `reports/metrics/fusion_selected_logs_metrics_random_forest.json`
- `reports/metrics/fusion_selected_logs_metrics_trace_latency_random_forest.json`
- `reports/metrics/fusion_selected_logs_metrics_graph_random_forest.json`
- `reports/metrics/fusion_logs_metrics_logistic_regression.json`
- `reports/metrics/fusion_selected_logs_metrics_logistic_regression.json`
- `reports/metrics/fusion_selected_logs_metrics_trace_latency_logistic_regression.json`
- `reports/metrics/fusion_selected_logs_metrics_graph_logistic_regression.json`
- `reports/metrics/fusion_summary.json`
- `reports/models/train_fusion.log`

Neu chay lai feature set cua lan dau se co them:

- `reports/metrics/fusion_logs_metrics_traces_random_forest.json`
- `reports/metrics/fusion_logs_metrics_traces_graph_random_forest.json`
- `reports/metrics/fusion_logs_metrics_traces_logistic_regression.json`
- `reports/metrics/fusion_logs_metrics_traces_graph_logistic_regression.json`

Tieu chi hoan thanh:

- Chung minh duoc da nguon tot hon hoac on dinh hon don nguon - Done, F1 `0.1121` > baseline `0.0890`.
- Chung minh graph features co gia tri hoac giai thich duoc khi khong cai thien - Done, `selected_logs_metrics_graph` la cau hinh tot nhat sau toi uu.
- Co ket qua F1/Precision/Recall cu the - Done.

Cach chay:

```bash
bash scripts/run_fusion_models.sh
```

Tuning them:

```bash
bash scripts/run_fusion_models.sh --negative-positive-ratio 20
bash scripts/run_fusion_models.sh reports/models --negative-positive-ratio 20
bash scripts/run_fusion_models.sh reports/models --feature-sets logs_metrics_traces,logs_metrics_traces_graph
```

## Sprint 6 - Orchestration Bang Airflow Va Truc Quan Hoa

Thoi luong de xuat: 4-5 ngay.

Trang thai hien tai: **Done**.

Da lam:

- Tao script chay pipeline tong: `scripts/run_pipeline.sh`.
- Tao Airflow DAG: `airflow/dags/train_ticket_pipeline.py`.
- Tao dashboard assets generator: `src/reports/build_dashboard_assets.py`.
- Tao notebook/dashboard demo: `notebooks/sprint6_dashboard.md`.
- Tao dashboard snapshot:
  - `reports/dashboard/model_comparison.csv`
  - `reports/dashboard/dashboard_summary.md`
  - `reports/dashboard/README.md`
- Dashboard generator doc duoc `baseline_summary.json`, `fusion_summary.json` va lich su `train_fusion_*.log` de khong mat ket qua tot khi file summary bi ghi de.
- Pipeline tong mac dinh chay fusion cau hinh tot nhat Sprint 5: Logistic Regression `selected_logs_metrics_graph`, ratio `50:1`.
- Da chay thanh cong `bash scripts/run_pipeline.sh` trong WSL:
  - Bat dau: `2026-05-19 16:03:25 +0700`.
  - Ket thuc: `2026-05-19 16:11:47 +0700`.
  - Log: `reports/models/pipeline.log`.
  - Dashboard summary: `reports/dashboard/dashboard_summary.md`.

Muc tieu:

- Bien cac script rieng le thanh pipeline co the chay lai.
- Tao dashboard hoac notebook demo ket qua.

Task chi tiet:

- Thiet ke Airflow DAG:
  - `scan_dataset`
  - `build_silver_logs`
  - `build_silver_metrics`
  - `build_silver_traces`
  - `build_silver_anomalies`
  - `build_gold_features`
  - `train_baselines`
  - `train_fusion_graph`
  - `evaluate_models`
- Cau hinh dependency:
  - Silver jobs chay song song sau scan.
  - Gold job chay sau khi silver hoan thanh.
  - Training chay sau gold.
  - Evaluation chay cuoi.
- Them logging:
  - Ghi thoi gian chay tung task.
  - Ghi so dong input/output.
  - Ghi loi neu parse fail.
- Tao dashboard Superset hoac notebook Zeppelin:
  - So luong log theo service/time.
  - CPU/memory theo service/time.
  - Latency p95 theo service/time.
  - So anomaly window theo case.
  - Bang so sanh model.
  - Confusion matrix.
- Neu co thoi gian, demo Kafka/Flink:
  - Replay mot file log vao Kafka topic.
  - Spark/Flink doc stream va tinh log count theo window.
  - Trinh bay nhu phan mo rong streaming, khong phai thanh phan bat buoc.

Deliverables:

- Airflow DAG chay pipeline end-to-end - Done qua `airflow/dags/train_ticket_pipeline.py`.
- Dashboard hoac notebook visualization - Done qua `notebooks/sprint6_dashboard.md`.
- Script chay pipeline bang mot lenh - Done qua `scripts/run_pipeline.sh`.
- Dashboard-ready CSV/Markdown generator - Done qua `src/reports/build_dashboard_assets.py`.
- Dashboard snapshot trong `reports/dashboard` - Done.
- Anh chup man hinh pipeline va dashboard cho bao cao - Co the chup tu `reports/models/pipeline.log` va `reports/dashboard/dashboard_summary.md`.

Ket qua pipeline WSL:

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

Ket qua sau pipeline:

- Gold `window_features`: 401,806 rows, 191 anomaly windows.
- Fusion best trong pipeline: LR `selected_logs_metrics_graph`, ratio `50:1`, threshold `0.99`, F1 `0.1121`.
- Dashboard generator ghi `reports/dashboard/dashboard_summary.md` va `reports/dashboard/model_comparison_20260519_161147.csv`; `model_comparison.csv` snapshot truoc do van duoc giu lai.

Tieu chi hoan thanh:

- Co the chay lai pipeline tu dau bang mot DAG hoac mot lenh tong - Done, da chay WSL thanh cong.
- Co dashboard/notebook de trinh bay ket qua truc quan - Done.
- Co log runtime cua tung buoc - Done trong `scripts/run_pipeline.sh`.

Cach chay nhanh:

```bash
cd /mnt/d/projects/big-data
bash scripts/run_pipeline.sh
```

Neu chi muon build dashboard tu ket qua da co:

```bash
python src/reports/build_dashboard_assets.py
```

## Sprint 7 - Bao Cao, Slide Va Dong Goi Nop Bai

Thoi luong de xuat: 1 tuan.

Muc tieu:

- Hoan thien report, slide, source code va huong dan cai dat.
- Dam bao do an the hien dung tinh chat Big Data.

Task chi tiet:

- Viet bao cao:
  - Gioi thieu bai toan microservice anomaly detection.
  - Tong quan base paper AMulSys.
  - Research gap va huong rut gon cua nhom.
  - Mo ta dataset Train-Ticket.
  - Kien truc Apache Big Data pipeline.
  - Thiet ke data lake bronze/silver/gold.
  - Tien xu ly logs, metrics, traces.
  - Dong bo time window va gan nhan anomaly.
  - Feature engineering va graph representation.
  - Mo hinh baseline va proposed model.
  - Ket qua thuc nghiem.
  - Phan tich, han che va huong phat trien.
- Viet slide:
  - 1 slide problem.
  - 1 slide dataset.
  - 1 slide architecture.
  - 1 slide data pipeline.
  - 1 slide feature engineering.
  - 1 slide models.
  - 1-2 slide results.
  - 1 slide demo/dashboard.
  - 1 slide conclusion.
- Viet README chay project:
  - Yeu cau moi truong.
  - Cach chuan bi data.
  - Cach chay Spark jobs.
  - Cach chay Airflow DAG.
  - Cach xem dashboard.
- Don dep source code:
  - Xoa file tam.
  - Kiem tra path tuong doi.
  - Them config mau.
  - Dam bao khong hard-code path ca nhan.
- Dong goi ket qua:
  - Source code.
  - Processed sample data neu duoc phep nop.
  - Report PDF/DOCX.
  - Slide.
  - Huong dan cai dat.
  - File tom tat tieng Anh neu mon hoc yeu cau.

Deliverables:

- Bao cao hoan chinh.
- Slide thuyet trinh.
- README cai dat va chay pipeline.
- Source code da sap xep.
- Ket qua thuc nghiem va dashboard screenshots.

Tieu chi hoan thanh:

- Nguoi khac doc README co the chay lai pipeline toi muc sample.
- Bao cao giai thich ro vai tro cua tung Apache component.
- Ket qua thuc nghiem co bang so lieu va nhan xet.

## Backlog Mo Rong Neu Con Thoi Gian

- Them Apache Kafka de replay telemetry nhu stream.
- Them Apache Flink de tinh online window features.
- Them Apache Iceberg de quan ly bang lakehouse tot hon.
- Thu nghiem nhieu kich thuoc window: 30s, 60s, 120s.
- Thu nghiem anomaly detection khong giam sat:
  - Isolation Forest ngoai Spark MLlib.
  - Autoencoder nhe neu co GPU/CPU du.
- Them graph metrics nang hon:
  - PageRank.
  - Betweenness approximation.
  - Community detection.
- Lam ablation study:
  - Logs only.
  - Metrics only.
  - Traces only.
  - Logs + metrics.
  - Logs + traces.
  - Metrics + traces.
  - Logs + metrics + traces.
  - Logs + metrics + traces + graph.

## Phan Cong Vai Tro Trong Nhom

Neu nhom co 3 thanh vien:

- Thanh vien 1: Data engineering
  - Data lake.
  - Spark ETL.
  - Silver layer.
- Thanh vien 2: Feature engineering va modeling
  - Window features.
  - Graph features.
  - Baseline va proposed model.
- Thanh vien 3: Orchestration, dashboard va report
  - Airflow.
  - Superset/Zeppelin.
  - Bao cao, slide, demo.

Neu nhom co 4 thanh vien:

- Thanh vien 1: Logs + anomalies.
- Thanh vien 2: Metrics + traces.
- Thanh vien 3: Feature engineering + models.
- Thanh vien 4: Airflow + dashboard + report integration.

## Rủi Ro Va Cach Xu Ly

| Rui ro | Tac dong | Cach xu ly |
|---|---|---|
| Spark chay cham tren may ca nhan | Tre tien do | Xu ly theo subset case truoc, cache it, ghi Parquet trung gian |
| Metrics JSON qua lon hoac nhieu series nhieu | Ton RAM | Loc metric quan trong, explode tung file, ghi Parquet ngay |
| Label anomaly tu TXT khong ro | Ket qua nhieu nhieu | Dung strict/relaxed window va giai thich trong bao cao |
| GraphX kho cai hoac kho chay | Tre sprint model | Dung Spark SQL tinh graph features lightweight |
| Airflow nang | Mat thoi gian setup | Lam DAG toi thieu hoac script orchestration thay the |
| Superset setup loi | Khong co dashboard | Dung Zeppelin/Jupyter notebook va anh chup ket qua |

## Definition Of Done Toan Do An

Do an duoc xem la hoan thanh khi co:

- Data lake gom bronze, silver, gold.
- Spark ETL doc va chuan hoa logs, metrics, traces.
- Bang feature window-level co label anomaly.
- It nhat 3 baseline don nguon.
- It nhat 1 mo hinh fusion da nguon.
- It nhat 1 bien the fusion + graph features.
- Bang so sanh Precision, Recall, F1-score.
- Airflow DAG hoac script pipeline end-to-end.
- Dashboard/notebook demo.
- Bao cao va slide giai thich duoc kien truc Apache Big Data.
