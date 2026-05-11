# Sprint 0 Summary - Khoi Dong Va Chuan Hoa Pham Vi

## 1. Trang Thai Sprint

Trang thai: Hoan thanh.

Sprint 0 da chot duoc:

- De tai va pham vi thuc hien.
- Paper nen va research gap.
- Dataset chinh.
- Kien truc Apache Big Data pipeline.
- Don vi phan tich theo time window.
- Dau ra ky thuat can co cho cac sprint tiep theo.

## 2. Ten De Tai

Ten de tai de xuat:

**Phat hien bat thuong trong he thong microservice bang phan tich du lieu telemetry da nguon va bieu dien do thi lai tren nen tang Apache Big Data**

Y nghia:

- "Microservice" the hien boi canh he thong phan tan.
- "Telemetry da nguon" gom logs, metrics va traces.
- "Bieu dien do thi lai" bam sat tinh than base paper AMulSys.
- "Apache Big Data" nhan manh pipeline xu ly du lieu lon, khong chi la mo hinh machine learning.

## 3. Base Paper

Paper nen:

Peipeng Wang, Xiuguo Zhang, Zhiying Cao, **Anomaly detection for microservice system via augmented multimodal data and hybrid graph representations**, Information Fusion, Volume 118, June 2025, Article 103017.

Y tuong chinh cua paper:

- Bai toan: phat hien bat thuong trong he thong microservice.
- Du lieu: ket hop 3 nguon telemetry gom logs, metrics va traces.
- Huong tiep can: AMulSys su dung multimodal data, data augmentation va hybrid graph representations.
- Diem manh: khai thac duoc thong tin bo sung giua cac nguon du lieu.
- Ket qua: paper bao cao F1-score cao, tren 0.97 trong phan abstract.

Pham vi tai hien trong do an:

- Khong tai hien day du AMulSys vi pipeline va mo hinh goc phuc tap.
- Tap trung xay dung phien ban nhe, de tai hien:
  - Xu ly logs, metrics, traces.
  - Dong bo theo time window.
  - Trich xuat feature da nguon.
  - Them graph features tu service-call traces.
  - So sanh baseline don nguon voi fusion da nguon va fusion + graph.

## 4. Research Gap

Khoang trong ma do an tap trung:

AMulSys dat ket qua cao nhung pipeline va mo hinh tuong doi phuc tap, yeu cau chi phi tien xu ly va tinh toan lon. Trong boi canh do an mon hoc voi tai nguyen han che, van can mot pipeline nhe hon, de tai hien hon, nhung van khai thac duoc gia tri cua logs, metrics va traces.

Huong giai quyet cua nhom:

- Xay dung pipeline Apache Big Data co cau truc ro rang.
- Dung Spark de xu ly du lieu da nguon.
- Dong bo du lieu theo time window.
- Tao baseline don nguon va mo hinh fusion da nguon.
- Bo sung graph features lightweight tu distributed traces.
- Danh gia bang Precision, Recall, F1-score va thoi gian xu ly.

## 5. Dataset Chinh

Dataset: Train-Ticket anomaly dataset.

Du lieu hien co trong repo:

```text
data/raw/train-ticket/
```

Dataset gom 10 case:

| Case | Service chinh | Cong nghe/phien ban | Ngay |
|---|---|---|---|
| case_01 | admin-basic-info | Spring Web 1.5.22 | 2022-07-08 |
| case_02 | auth | MongoDB 4.4.15 | 2022-07-13 |
| case_03 | auth | MongoDB 5.0.9 | 2022-07-06 |
| case_04 | auth | MongoDB 4.4.15 | 2022-07-27 |
| case_05 | order | Spring Boot 2.7.1 | 2022-07-11 |
| case_06 | order | MongoDB Driver 3.0.4 | 2022-07-13 |
| case_07 | order | MongoDB 4.2.2 | 2022-07-12 |
| case_08 | order | MongoDB 4.4.15 | 2022-07-12 |
| case_09 | order | Spring Data MongoDB 1.5.22 | 2022-07-11 |
| case_10 | order | Spring Data MongoDB 2.0.0 | 2022-07-11 |

Ly do chon dataset:

- Co du 3 nguon du lieu theo paper nen: logs, metrics, traces.
- Co nhan anomaly duoi dang `potentialAnomalies_*.txt`.
- Quy mo vua phai, phu hop voi may ca nhan 16GB RAM va 20GB disk.
- Gan voi he benchmark Train-Ticket gom nhieu microservices.

## 6. Mo Ta Nguon Du Lieu Va Schema Du Kien

### 6.1 Logs

Nguon:

```text
LOGS_<service_name>.txt_structured.csv
```

Cot goc quan trong:

```text
LineId, Date, Time, Level, Number, LoggingReporter, Content, EventId, EventTemplate, ParameterList
```

Bang silver du kien: `silver_logs`

| Cot | Mo ta |
|---|---|
| case_id | Ma case, vi du `case_07` |
| service_name | Service phat sinh log |
| timestamp | Thoi diem log |
| level | INFO, WARN, ERROR |
| event_id | Ma template event |
| event_template | Template log |
| content | Noi dung log |
| is_error | 1 neu level/error content the hien loi |
| is_warn | 1 neu level la WARN |
| is_span_reported | 1 neu content co `Span reported` |

Feature logs du kien:

- `log_count`
- `error_count`
- `warn_count`
- `unique_event_id_count`
- `span_reported_count`
- `top_event_frequency`
- `template_entropy`

### 6.2 Metrics

Nguon:

```text
Monitoring_<service_name>.json_<date>/*.json
```

Dinh dang: Prometheus JSON response.

Bang silver du kien: `silver_metrics`

| Cot | Mo ta |
|---|---|
| case_id | Ma case |
| metric_name | Ten metric Prometheus |
| timestamp | Unix timestamp da chuan hoa |
| value | Gia tri metric |
| container | Container name neu co |
| pod | Pod name neu co |
| namespace | Kubernetes namespace |
| node | Node name |

Metric uu tien:

- `container_cpu_usage_seconds_total`
- `container_memory_working_set_bytes`
- `container_network_transmit_packets_total`
- `node_cpu_seconds_total`
- `node_memory_MemAvailable_bytes`
- `node_memory_MemTotal_bytes`
- `node_namespace_pod_container_container_cpu_usage_seconds_total_sum_irate`
- `node_namespace_pod_container_container_memory_working_set_bytes`

Feature metrics du kien:

- `cpu_mean`, `cpu_max`, `cpu_std`
- `memory_mean`, `memory_max`, `memory_std`
- `network_mean`, `network_max`
- `node_memory_available_mean`
- `cpu_rate_mean`

### 6.3 Distributed Traces

Nguon:

```text
Traces_<service_name>_<date>/*.json
```

Dinh dang: Jaeger JSON response.

Bang silver du kien: `silver_spans`

| Cot | Mo ta |
|---|---|
| case_id | Ma case |
| trace_id | ID trace |
| span_id | ID span |
| parent_span_id | ID span cha |
| service_name | Service sinh span |
| operation_name | Operation cua span |
| start_time | Thoi diem bat dau |
| duration | Thoi gian thuc thi |
| http_status | HTTP status code |
| http_method | GET, POST, PUT, DELETE |
| http_url | URL neu co |
| is_error | Co tag error hoac status 5xx |

Bang edge du kien: `silver_trace_edges`

| Cot | Mo ta |
|---|---|
| case_id | Ma case |
| trace_id | ID trace |
| source_service | Service goi |
| target_service | Service duoc goi |
| operation_name | Operation |
| duration | Do tre |
| is_error | Edge co loi hay khong |

Feature traces du kien:

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

Feature graph du kien:

- `in_degree`
- `out_degree`
- `weighted_call_count`
- `avg_edge_latency`
- `max_edge_latency`
- `error_edge_count`
- Optional: `pagerank_score` neu GraphX chay on dinh.

### 6.4 Anomalies

Nguon:

```text
potentialAnomalies_<service_name>.txt
```

Bang silver du kien: `silver_anomaly_events`

| Cot | Mo ta |
|---|---|
| case_id | Ma case |
| service_name | Service lien quan |
| anomaly_timestamp | Timestamp trich xuat tu text |
| raw_text | Dong/nhom text goc |

Cach gan nhan:

- Strict label: `label = 1` neu window chua anomaly timestamp.
- Relaxed label: `label = 1` neu window nam trong khoang anomaly timestamp +- 60s hoac +- 120s.
- Mac dinh sprint dau: dung relaxed +- 60s de giam rui ro lech timestamp.

## 7. Don Vi Phan Tich Duoc Chot

Don vi phan tich: **service-level time window**.

Khoa logic:

```text
case_id, service_name, window_start, window_end
```

Window size mac dinh:

```text
60 seconds
```

Ly do:

- Metrics trong dataset co tan suat lay mau xap xi 30 giay.
- Window 60 giay du rong de gom logs, traces va metrics.
- Giam nhieu so voi window 30 giay trong giai do dau.
- Phu hop tai nguyen may ca nhan.

Co the thu nghiem them:

```text
30 seconds
120 seconds
```

nhung chi de ablation neu con thoi gian.

## 8. Pham Vi Case Xu Ly

Quyet dinh:

- Sprint 1-3: xu ly duoc toan bo 10 case neu pipeline chay on dinh.
- Khi phat trien va debug: uu tien 3 case mau.

Subset debug de xuat:

```text
case_03_auth_mongo_5_0_9_20220706
case_07_order_mongodb_4_2_2_20220712
case_10_order_springdata_mongodb_2_0_0_20220711
```

Ly do chon subset:

- Co ca auth va order service.
- Co du logs, metrics, traces, anomaly text.
- Dai dien cho nhieu phien ban MongoDB/Spring Data.

Pham vi report cuoi:

- Neu may chay tot: bao cao ket qua tren 10 case.
- Neu tai nguyen han che: bao cao ket qua tren subset 3-5 case va giai thich gioi han tai nguyen.

## 9. Kien Truc Apache Big Data Pipeline

Kien truc tong the:

```text
Raw Train-Ticket Dataset
        |
        v
Bronze Data Lake
logs / metrics / traces / anomalies
        |
        v
Apache Spark ETL
parse + clean + normalize
        |
        v
Silver Tables
silver_logs / silver_metrics / silver_spans / silver_trace_edges / silver_anomalies
        |
        v
Apache Spark Feature Engineering
windowing + fusion + graph features
        |
        v
Gold Feature Table
gold_window_features
        |
        v
Spark MLlib
baselines + fusion + graph-enhanced model
        |
        v
Evaluation + Dashboard
Precision / Recall / F1 / runtime / visualization
```

Vai tro Apache components:

| Component | Vai tro |
|---|---|
| Apache Spark | Xu ly ETL, feature engineering, MLlib |
| Hadoop/HDFS hoac local data lake | Luu bronze/silver/gold data |
| Hive/Spark SQL | Truy van bang silver/gold |
| Airflow | Orchestration pipeline |
| Superset/Zeppelin | Visualization va demo ket qua |
| Kafka/Flink | Optional cho demo streaming neu con thoi gian |

Quyet dinh thuc te:

- Bat dau voi Spark local mode va local data lake.
- Khong bat buoc chay full Hadoop cluster.
- Kafka/Flink chi la backlog mo rong, khong nam tren critical path.

## 10. Bang Gold Cuoi Cung

Bang chinh de train model:

```text
gold_window_features
```

Schema logic:

| Nhom cot | Vi du |
|---|---|
| Key | `case_id`, `service_name`, `window_start`, `window_end` |
| Log features | `log_count`, `error_count`, `unique_event_id_count` |
| Metric features | `cpu_mean`, `memory_max`, `network_mean` |
| Trace features | `trace_count`, `p95_duration`, `http_5xx_count` |
| Graph features | `in_degree`, `out_degree`, `avg_edge_latency` |
| Label | `label` |

Muc tieu cua bang gold:

- La dau vao duy nhat cho cac model.
- Cho phep so sanh don nguon va da nguon bang cach chon tap cot khac nhau.
- Co the truy van va visualize bang Spark SQL/Superset.

## 11. Thuc Nghiem Du Kien

Model baseline:

| Model | Feature |
|---|---|
| Logs-only | Chi log features |
| Metrics-only | Chi metric features |
| Traces-only | Chi trace features |
| Fusion baseline | Logs + metrics + traces |
| Proposed lightweight | Logs + metrics + traces + graph features |

Thuat toan uu tien:

- Logistic Regression.
- Random Forest.
- Gradient-Boosted Trees neu tai nguyen cho phep.

Metric danh gia:

- Precision.
- Recall.
- F1-score.
- Confusion matrix.
- Training time.
- Processing time.

## 12. Dau Ra Can Co Sau Cac Sprint

Cuoi do an can co:

- Source code Spark ETL.
- Data lake layout bronze/silver/gold.
- Bang `gold_window_features`.
- Ket qua baseline va proposed model.
- Airflow DAG hoac script pipeline end-to-end.
- Dashboard hoac notebook demo.
- Bao cao.
- Slide thuyet trinh.
- README huong dan chay.

## 13. Ranh Gioi Pham Vi

Trong pham vi:

- Xu ly logs, metrics, traces.
- Dong bo theo time window.
- Dung Spark cho ETL va ML.
- Tao graph features tu trace edges.
- So sanh don nguon va da nguon.

Ngoai pham vi bat buoc:

- Tai hien full AMulSys.
- Contrastive learning nang.
- Multimodal data augmentation phuc tap.
- Hadoop cluster that su nhieu node.
- Real-time streaming production-grade.
- Xu ly cac dataset rat lon nhu LO2 full hay GAIA full.

## 14. Definition Of Done Cho Sprint 0

Sprint 0 duoc xem la hoan thanh vi da co cac artifact/quyet dinh sau:

- Da co ten de tai chinh thuc.
- Da co tom tat paper nen.
- Da co research gap va huong giai quyet.
- Da chot dataset Train-Ticket.
- Da mo ta schema du kien cho logs, metrics, traces, anomalies.
- Da chot window size mac dinh 60s.
- Da chot subset debug va pham vi case.
- Da co kien truc Apache Big Data pipeline.
- Da co danh sach output cho cac sprint tiep theo.

Ket luan:

**Sprint 0 hoan thanh. Co the chuyen sang Sprint 1: Thiet lap moi truong va Data Lake.**
