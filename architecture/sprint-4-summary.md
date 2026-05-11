# Sprint 4 Summary - Baseline Models Don Nguon

## Trang Thai

Trang thai: Da chay baseline lan 1; can cai thien threshold/model trong pham vi Sprint 4 truoc khi sang Sprint 5.

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

Baseline hien tai dung Spark ML Logistic Regression:

- `VectorAssembler`
- `StandardScaler`
- `LogisticRegression`
- `weightCol=class_weight` de xu ly imbalance

## Ket Qua Baseline Lan 1

Split test co 14 anomaly tren 133136 windows, nen accuracy khong phai metric chinh.

| Baseline | Precision | Recall | F1-score | Nhan xet |
|---|---:|---:|---:|---|
| metrics-only | 0.0546 | 0.7143 | 0.1015 | Tot nhat hien tai theo F1 |
| logs-only | 0.0053 | 0.8571 | 0.0106 | Recall cao nhung false positive rat nhieu |
| traces-only | 0.0000 | 0.0000 | 0.0000 | Chua phan biet duoc anomaly |

Nhan xet:

- Metrics-only la baseline don nguon manh nhat trong lan chay dau.
- Logs-only co tin hieu anomaly nhung can giam false positive.
- Traces-only yeu, co the do trace coverage thieu hoac feature trace chua du phan biet.
- Area under ROC cua metrics-only cao, nen can thu threshold tuning truoc khi ket luan model yeu.

## Cai Thien Trong Sprint 4

Nhung viec can lam tiep trong Sprint 4, chua chuyen sang fusion cua Sprint 5:

- Them threshold tuning cho Logistic Regression:
  - Quet threshold tren predicted probability.
  - Luu `best_threshold`, `best_precision`, `best_recall`, `best_f1`.
  - Uu tien threshold toi uu F1, dong thoi bao cao trade-off Precision/Recall.
- Them Random Forest baseline don nguon neu tai nguyen cho phep:
  - Train logs-only, metrics-only, traces-only voi cung split theo case.
  - Luu feature importance de giai thich nguon tin hieu.
- Chua lam fusion trong Sprint 4:
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

- `bash scripts/run_baseline_models.sh` chay thanh cong.
- Co 3 file ket qua baseline don nguon trong `reports/metrics`.
- Moi baseline co Precision, Recall va F1-score.
- Co confusion matrix cho logs-only, metrics-only va traces-only.
- Co nhan xet baseline nao manh/yeu hon dua tren metrics.
- Co threshold tuning cho Logistic Regression baseline.
- Neu kip, co Random Forest baseline don nguon va feature importance.

## Luu Y

- Label gold dang rat lech lop: 40 anomaly tren 401806 windows.
- Ket qua baseline dau tien chu yeu la moc so sanh; can tune threshold de danh gia cong bang hon truoc Sprint 5.
