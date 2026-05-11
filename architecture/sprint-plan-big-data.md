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

- `silver_logs`
- `silver_metrics`
- `silver_spans`
- `silver_trace_edges`
- `silver_anomaly_events`

Tieu chi hoan thanh:

- Tat ca bang silver doc duoc bang Spark SQL.
- Timestamp da duoc chuan hoa.
- Moi bang deu co `case_id`.
- Co notebook hoac script kiem tra sample data cua tung bang.

## Sprint 3 - Gold Layer: Windowing, Labels Va Feature Engineering

Thoi luong de xuat: 1 tuan.

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
  - Neu anomaly timestamp nam trong window thi `label = 1`.
  - Thu nghiem label relaxed: anomaly timestamp +- 60s hoac +- 120s.
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

- Bang `gold_window_features`.
- Data dictionary cho tat ca cot feature.
- Script thong ke class imbalance.
- Bao cao ngan ve so window anomaly va normal.

Tieu chi hoan thanh:

- Mot dong feature dai dien duoc mot service trong mot time window.
- Co label anomaly.
- Train/test split co the thuc hien truc tiep tu bang gold.

## Sprint 4 - Baseline Models Don Nguon

Thoi luong de xuat: 4-5 ngay.

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
- Danh gia:
  - Precision.
  - Recall.
  - F1-score.
  - Confusion matrix.
  - Training time.
  - Inference time.
- Luu ket qua:
  - `reports/metrics/baseline_logs.json`
  - `reports/metrics/baseline_metrics.json`
  - `reports/metrics/baseline_traces.json`

Deliverables:

- Ket qua 3 baseline don nguon.
- Bang so sanh logs-only, metrics-only, traces-only.
- Nhan xet nguon du lieu nao manh/yey hon.

Tieu chi hoan thanh:

- Co it nhat 3 baseline chay thanh cong.
- Co bang metric ro rang de dua vao bao cao.
- Co nhan xet ve uu/nhuoc diem cua tung nguon telemetry.

## Sprint 5 - Fusion Da Nguon Va Graph-Enhanced Model

Thoi luong de xuat: 1 tuan.

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

- Ket qua fusion baseline.
- Ket qua graph-enhanced model.
- Bang feature importance.
- Bang tong hop tat ca model.

Tieu chi hoan thanh:

- Chung minh duoc da nguon tot hon hoac on dinh hon don nguon.
- Chung minh graph features co gia tri hoac giai thich duoc khi khong cai thien.
- Co ket qua F1/Precision/Recall cu the.

## Sprint 6 - Orchestration Bang Airflow Va Truc Quan Hoa

Thoi luong de xuat: 4-5 ngay.

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

- Airflow DAG chay pipeline end-to-end.
- Dashboard hoac notebook visualization.
- Anh chup man hinh pipeline va dashboard cho bao cao.

Tieu chi hoan thanh:

- Co the chay lai pipeline tu dau bang mot DAG hoac mot lenh tong.
- Co dashboard/notebook de trinh bay ket qua truc quan.
- Co log runtime cua tung buoc.

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
