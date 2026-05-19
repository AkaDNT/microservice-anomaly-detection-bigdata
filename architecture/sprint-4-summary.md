# Sprint 4 Summary - Baseline Models Don Nguon

## Trang Thai

Trang thai: Hoan thanh Sprint 4. Da rebuild baseline sau khi noi label gold relaxed 120s; co du Logistic Regression va Random Forest cho logs-only, metrics-only va traces-only.

Sprint 4 dung bang gold window-level de train 3 baseline don nguon:

```text
data_lake/gold/window_features
```

Split mac dinh:

```text
train: case_01 -> case_07
test:  case_08 -> case_10
```

Split theo `case_id` giup giam leakage giua train va test.

## Artifact Da Tao

Code model:

- `src/models/train_baselines.py`

Scripts WSL:

- `scripts/run_baseline_models.sh`

Output metrics:

- `reports/metrics/baseline_logs.json`
- `reports/metrics/baseline_metrics.json`
- `reports/metrics/baseline_traces.json`
- `reports/metrics/baseline_logs_random_forest.json`
- `reports/metrics/baseline_metrics_random_forest.json`
- `reports/metrics/baseline_traces_random_forest.json`
- `reports/metrics/baseline_summary.json`

Log runtime:

- `reports/models/train_baselines_<timestamp>.log`
- `reports/models/train_baselines.log`

## Baseline Groups

### Logs-only

- `log_count`
- `error_count`
- `warn_count`
- `info_count`
- `unique_event_id_count`
- `span_reported_count`
- `top_event_frequency`
- `template_entropy`

### Metrics-only

- `cpu_mean`
- `cpu_max`
- `cpu_std`
- `memory_mean`
- `memory_max`
- `memory_std`
- `network_mean`
- `network_max`
- `node_memory_available_mean`
- `node_memory_total_mean`
- `cpu_rate_mean`

### Traces-only

- `trace_count`
- `span_count`
- `avg_duration_ms`
- `max_duration_ms`
- `p95_duration_ms`
- `error_span_count`
- `http_4xx_count`
- `http_5xx_count`
- `unique_operation_count`

## Model

Baseline hien tai dung Spark ML:

- `VectorAssembler`
- `StandardScaler`
- `LogisticRegression`
- `RandomForestClassifier`
- `weightCol=class_weight` de xu ly imbalance

## Ket Qua Baseline Sau Rebuild Label 120s

Gold sau rebuild co 191 anomaly windows tren 401,806 windows.

Split theo case:

| Split | Rows | Label 0 | Label 1 |
|---|---:|---:|---:|
| Train `case_01` -> `case_07` | 268,670 | 268,509 | 161 |
| Test `case_08` -> `case_10` | 133,136 | 133,106 | 30 |

Test set van rat lech lop, nen accuracy khong phai metric chinh.

Ket qua tai threshold mac dinh cua Logistic Regression:

| Baseline | Precision | Recall | F1-score | Nhan xet |
|---|---:|---:|---:|---|
| logs-only | 0.0105 | 0.7667 | 0.0206 | Bat duoc 23/30 anomaly nhung false positive cao |
| metrics-only | 0.0022 | 0.7667 | 0.0045 | Recall cao nhung false positive rat cao o threshold mac dinh |
| traces-only | 0.0000 | 0.0000 | 0.0000 | Chua phan biet duoc anomaly |

Ket qua threshold tuning tot nhat theo F1:

| Algorithm | Baseline | Threshold | Precision | Recall | F1-score | TP | FP | FN | TN |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Logistic Regression | metrics-only | 0.90 | 0.0472 | 0.7667 | 0.0890 | 23 | 464 | 7 | 132,642 |
| Logistic Regression | logs-only | 0.96 | 0.0533 | 0.2667 | 0.0889 | 8 | 142 | 22 | 132,964 |
| Logistic Regression | traces-only | 0.43 | 0.0002 | 1.0000 | 0.0005 | 30 | 133,100 | 0 | 6 |
| Random Forest | logs-only | 0.93 | 0.0441 | 0.3000 | 0.0769 | 9 | 195 | 21 | 132,911 |
| Random Forest | metrics-only | 0.61 | 0.0052 | 0.9667 | 0.0103 | 29 | 5,581 | 1 | 127,525 |
| Random Forest | traces-only | 0.42 | 0.0002 | 1.0000 | 0.0005 | 30 | 132,783 | 0 | 323 |

Ket qua Random Forest tai threshold mac dinh:

| Baseline | Precision | Recall | F1-score | TP | FP | FN | TN | ROC AUC |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| logs-only | 0.0283 | 0.6000 | 0.0541 | 18 | 618 | 12 | 132,488 | 0.7015 |
| metrics-only | 0.0051 | 0.9667 | 0.0101 | 29 | 5,697 | 1 | 127,409 | 0.9621 |
| traces-only | 0.0000 | 0.0000 | 0.0000 | 0 | 572 | 30 | 132,534 | 0.4988 |

Feature importance chinh cua Random Forest:

| Baseline | Top features |
|---|---|
| logs-only | `span_reported_count`, `top_event_frequency`, `info_count`, `log_count` |
| metrics-only | `memory_mean`, `cpu_max`, `memory_std`, `cpu_mean`, `memory_max` |
| traces-only | `p95_duration_ms`, `avg_duration_ms`, `span_count`, `max_duration_ms` |

Nhan xet:

- Logistic Regression metrics-only va logs-only gan nhu ngang F1 sau threshold tuning, nhung trade-off khac nhau.
- Logistic Regression metrics-only giu recall 0.7667 voi 464 false positives, phu hop hon neu uu tien bat anomaly.
- Logistic Regression logs-only giam false positive xuong 142 nhung recall chi con 0.2667.
- Random Forest logs-only tot hon Random Forest metrics-only theo F1 tai threshold mac dinh, nhung van thua Logistic Regression sau threshold tuning.
- Random Forest metrics-only co ROC AUC cao va recall rat cao, nhung precision thap do false positive rat lon.
- Traces-only yeu, co the do trace coverage thieu hoac feature trace chua du phan biet.
- Baseline van yeu ve precision vi imbalance lon; can fusion feature va/hoac model manh hon o Sprint 5.

## Hoan Thanh Trong Sprint 4

Nhung viec da hoan thanh trong Sprint 4:

- Them threshold tuning cho Logistic Regression:
  - Quet threshold tren predicted probability.
  - Luu `best_threshold`, `best_precision`, `best_recall`, `best_f1`.
  - Uu tien threshold toi uu F1, dong thoi bao cao trade-off Precision/Recall.
- Them Random Forest baseline don nguon:
  - Train logs-only, metrics-only, traces-only voi cung split theo case.
  - Luu feature importance de giai thich nguon tin hieu.
- Chua lam fusion trong Sprint 4, de lai cho Sprint 5:
  - `logs + metrics`
  - `logs + metrics + traces`
  - `logs + metrics + traces + graph`
  - Cac muc nay de lai cho Sprint 5.

Metrics duoc luu:

- Precision
- Recall
- F1-score
- Accuracy
- Confusion matrix: TP, FP, FN, TN
- Area under ROC
- Area under PR

## Cach Chay Trong WSL

```bash
cd /mnt/d/projects/big-data
bash scripts/run_baseline_models.sh
```

Script mac dinh train Logistic Regression va tu dong quet threshold 0.01 den 0.99 cho tung baseline.

Neu muon chay them Random Forest baseline:

```bash
.venv/bin/spark-submit src/models/train_baselines.py --include-random-forest
```

Neu muon chi dinh train cases:

```bash
.venv/bin/spark-submit src/models/train_baselines.py \
  --train-cases case_01_admin_basic_info_spring_1_5_22,case_02_auth_mongo_4_4_15_20220713
```

## Definition Of Done Sprint 4

Sprint 4 dat DoD khi:

| Tieu chi | Trang thai |
|---|---|
| `bash scripts/run_baseline_models.sh` chay thanh cong | Done |
| Co 3 file ket qua Logistic Regression don nguon trong `reports/metrics` | Done |
| Moi baseline co Precision, Recall va F1-score | Done |
| Co confusion matrix cho logs-only, metrics-only va traces-only | Done |
| Co nhan xet baseline nao manh/yeu hon dua tren metrics | Done |
| Co threshold tuning cho Logistic Regression baseline | Done |
| Co Random Forest baseline don nguon va feature importance | Done |

Ket luan: **Sprint 4 Done**.

## Luu Y

- Label gold ban dau rat lech lop: 40 anomaly tren 401,806 windows voi logic relaxed 60s theo service tu ten file.
- Labeling da duoc noi len relaxed 120s, uu tien service suy luan tu anomaly text va bo sung service tren cung trace id; ket qua moi la 191 anomaly tren 401,806 windows.
- Random Forest duoc chay truc tiep bang `.venv/bin/spark-submit src/models/train_baselines.py --include-random-forest`, nen `reports/models/train_baselines.log` van co the la log cua lan Logistic Regression truoc do; ket qua RF da duoc ghi trong `reports/metrics/*_random_forest.json` va `baseline_summary.json`.
- Ket qua baseline da co threshold tuning; Sprint 5 nen tap trung fusion logs + metrics + traces + graph va giam false positive.
