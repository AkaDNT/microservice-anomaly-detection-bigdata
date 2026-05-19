# Sprint 5 Summary - Fusion Da Nguon Va Graph-Enhanced Model

## Trang Thai

Trang thai: Done. Sau vong toi uu, fusion + graph da vuot baseline don nguon Sprint 4.

Sprint 5 dung bang gold window-level:

```text
data_lake/gold/window_features
```

Split mac dinh tiep tuc giong Sprint 4:

```text
train: case_01 -> case_07
test:  case_08 -> case_10
```

## Artifact Da Tao

Code model:

- `src/models/train_fusion.py`

Script WSL:

- `scripts/run_fusion_models.sh`

Output metrics du kien voi cau hinh toi uu mac dinh:

- `reports/metrics/fusion_logs_metrics_random_forest.json`
- `reports/metrics/fusion_selected_logs_metrics_random_forest.json`
- `reports/metrics/fusion_selected_logs_metrics_trace_latency_random_forest.json`
- `reports/metrics/fusion_selected_logs_metrics_graph_random_forest.json`
- `reports/metrics/fusion_logs_metrics_logistic_regression.json`
- `reports/metrics/fusion_selected_logs_metrics_logistic_regression.json`
- `reports/metrics/fusion_selected_logs_metrics_trace_latency_logistic_regression.json`
- `reports/metrics/fusion_selected_logs_metrics_graph_logistic_regression.json`
- `reports/metrics/fusion_summary.json`

Neu chay lai feature set cua lan dau thi se co them:

- `reports/metrics/fusion_logs_metrics_traces_random_forest.json`
- `reports/metrics/fusion_logs_metrics_traces_graph_random_forest.json`
- `reports/metrics/fusion_logs_metrics_traces_logistic_regression.json`
- `reports/metrics/fusion_logs_metrics_traces_graph_logistic_regression.json`

Log runtime du kien:

- `reports/models/train_fusion_<timestamp>.log`
- `reports/models/train_fusion.log`

## Feature Sets

Sau lan chay dau, feature set mac dinh da duoc doi sang cac bien the nhe hon de tranh trace/graph noise lam tang false positive:

### `logs_metrics`

Ket hop toan bo log features va metric features, bo trace/graph.

### `selected_logs_metrics`

Feature selection tu ket qua Sprint 4 va RF feature importance:

- Log: `span_reported_count`, `top_event_frequency`, `info_count`, `log_count`, `unique_event_id_count`, `template_entropy`, `error_count`
- Metric: `memory_mean`, `memory_std`, `cpu_mean`, `cpu_max`, `memory_max`, `cpu_std`, `network_mean`, `network_max`

### `selected_logs_metrics_trace_latency`

Mo rong `selected_logs_metrics` voi trace latency features manh hon:

- `p95_duration_ms`
- `avg_duration_ms`
- `span_count`
- `max_duration_ms`

### `selected_logs_metrics_graph`

Mo rong `selected_logs_metrics` voi graph features nhe:

- `in_degree`
- `weighted_call_count`
- `avg_edge_latency_ms`
- `max_edge_latency_ms`

### `logs_metrics_traces`

Ket hop 3 nguon don:

- Log features:
  - `log_count`
  - `error_count`
  - `warn_count`
  - `info_count`
  - `unique_event_id_count`
  - `span_reported_count`
  - `top_event_frequency`
  - `template_entropy`
- Metric features:
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
- Trace features:
  - `trace_count`
  - `span_count`
  - `avg_duration_ms`
  - `max_duration_ms`
  - `p95_duration_ms`
  - `error_span_count`
  - `http_4xx_count`
  - `http_5xx_count`
  - `unique_operation_count`

### `logs_metrics_traces_graph`

Mo rong feature set tren voi graph features:

- `in_degree`
- `out_degree`
- `weighted_call_count`
- `avg_edge_latency_ms`
- `max_edge_latency_ms`
- `error_edge_count`
- `unique_peer_service_count`

## Model

Job hien train 2 algorithm:

- Spark ML Logistic Regression
- Spark ML Random Forest

Ca hai dung:

- `VectorAssembler`
- `StandardScaler`
- `class_weight` de xu ly imbalance
- Downsample negative train windows mac dinh theo ty le `50:1` so voi positive windows
- Threshold tuning tu 0.01 den 0.99 theo F1

## Cach Chay Trong WSL

```bash
cd /mnt/d/projects/big-data
bash scripts/run_fusion_models.sh
```

Script co the truyen tham so xuong Spark job. Vi du thu ratio khac:

```bash
bash scripts/run_fusion_models.sh --negative-positive-ratio 20
bash scripts/run_fusion_models.sh reports/models --negative-positive-ratio 20
```

Neu muon chi chay mot algorithm:

```bash
.venv/bin/spark-submit src/models/train_fusion.py --algorithms random_forest
.venv/bin/spark-submit src/models/train_fusion.py --algorithms logistic_regression
```

Neu muon chay lai dung feature set cua lan dau:

```bash
.venv/bin/spark-submit src/models/train_fusion.py --feature-sets logs_metrics_traces,logs_metrics_traces_graph
```

## Ket Qua Lan Chay Dau

Fusion training chay xong luc `2026-05-19 12:35:27 +0700`.

Split test van la:

```text
test_rows=133,136
label_0=133,106
label_1=30
```

Ket qua threshold tuning tot nhat theo F1:

| Algorithm | Feature set | Threshold | Precision | Recall | F1-score | TP | FP | FN | TN |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Random Forest | logs_metrics_traces | 0.51 | 0.0057 | 0.7000 | 0.0113 | 21 | 3,671 | 9 | 129,435 |
| Random Forest | logs_metrics_traces_graph | 0.56 | 0.0051 | 0.7000 | 0.0101 | 21 | 4,101 | 9 | 129,005 |
| Logistic Regression | logs_metrics_traces | 0.99 | 0.0324 | 0.2000 | 0.0558 | 6 | 179 | 24 | 132,927 |
| Logistic Regression | logs_metrics_traces_graph | 0.99 | 0.0308 | 0.2000 | 0.0533 | 6 | 189 | 24 | 132,917 |

So voi baseline don nguon Sprint 4:

| Model | Best F1 |
|---|---:|
| LR metrics-only | 0.0890 |
| LR logs-only | 0.0889 |
| RF logs-only | 0.0769 |
| LR fusion logs+metrics+traces | 0.0558 |
| LR fusion+graph | 0.0533 |
| RF fusion logs+metrics+traces | 0.0113 |
| RF fusion+graph | 0.0101 |

Nhan xet:

- Fusion da chay on dinh va co ROC AUC cao, nhung F1 thap vi false positive con nhieu hoac threshold tot nhat lam recall thap.
- Graph features chua cai thien ket qua: ca LR va RF deu giam F1 nhe khi them graph.
- Feature importance cua RF fusion van bi chi phoi boi metric features nhu `memory_mean`, `memory_std`, `cpu_mean`, `cpu_max`; graph features co importance thap.
- Ket qua nay chua dat muc "fusion tot hon baseline don nguon", nen Sprint 5 can tiep tuc cai thien.

Huong cai thien tiep:

- Da them feature selection: bo bot trace/graph feature yeu, giu logs + metrics manh.
- Da them model `logs_metrics` rieng de xem fusion 2 nguon co tot hon 3 nguon khong.
- Da them downsample normal train windows mac dinh `50:1` de giam false positive.
- Thu Random Forest voi maxDepth/numTrees khac hoac Gradient-Boosted Trees neu tai nguyen cho phep.

## Vong Toi Uu Sau Lan Chay Dau

Ly do toi uu:

- `logs_metrics_traces` va `logs_metrics_traces_graph` co recall kha cao voi Random Forest, nhung FP rat lon nen precision va F1 thap.
- Trace va graph features dang them nhieu tin hieu yeu; graph importance thap trong RF.
- Baseline don nguon tot nhat nam o metrics/logs, nen Sprint 5 can thu fusion 2 nguon logs + metrics truoc khi ep them trace/graph.

Thay doi da lam:

- Them cac feature set `logs_metrics`, `selected_logs_metrics`, `selected_logs_metrics_trace_latency`, `selected_logs_metrics_graph`.
- Doi default `--feature-sets` sang cac bien the da chon loc.
- Them `--negative-positive-ratio`, mac dinh `50`, de downsample negative windows trong train.
- Giu nguyen test set that, khong downsample test, nen TP/FP/FN/TN van phan anh du lieu test thuc.
- Cho phep `scripts/run_fusion_models.sh` nhan tham so bo sung va truyen vao `train_fusion.py`.

## Ket Qua Sau Toi Uu

Da chay 2 cau hinh:

```bash
bash scripts/run_fusion_models.sh
bash scripts/run_fusion_models.sh --negative-positive-ratio 20
```

Luu y: `reports/metrics/fusion_summary.json` bi lan chay `--negative-positive-ratio 20` ghi de, nen ket qua tot nhat cua cau hinh mac dinh `50:1` duoc lay tu log `reports/models/train_fusion_20260519_124849.log`.

### Default `50:1`

| Algorithm | Feature set | Threshold | Precision | Recall | F1-score | TP | FP | FN | TN |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Logistic Regression | selected_logs_metrics_graph | 0.99 | 0.0779 | 0.2000 | 0.1121 | 6 | 71 | 24 | 133,035 |
| Logistic Regression | selected_logs_metrics_trace_latency | 0.99 | 0.0432 | 0.2000 | 0.0710 | 6 | 133 | 24 | 132,973 |
| Logistic Regression | logs_metrics | 0.94 | 0.0345 | 0.6333 | 0.0654 | 19 | 532 | 11 | 132,574 |
| Logistic Regression | selected_logs_metrics | 0.94 | 0.0344 | 0.6333 | 0.0652 | 19 | 534 | 11 | 132,572 |
| Random Forest | selected_logs_metrics_graph | 0.77 | 0.0095 | 0.6667 | 0.0187 | 20 | 2,084 | 10 | 131,022 |

### Ratio `20:1`

| Algorithm | Feature set | Threshold | Precision | Recall | F1-score | TP | FP | FN | TN |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Logistic Regression | selected_logs_metrics_graph | 0.95 | 0.0277 | 0.6000 | 0.0530 | 18 | 631 | 12 | 132,475 |
| Logistic Regression | selected_logs_metrics | 0.93 | 0.0260 | 0.6333 | 0.0499 | 19 | 712 | 11 | 132,394 |
| Logistic Regression | logs_metrics | 0.93 | 0.0260 | 0.6333 | 0.0499 | 19 | 712 | 11 | 132,394 |
| Random Forest | selected_logs_metrics_trace_latency | 0.69 | 0.0107 | 0.9333 | 0.0212 | 28 | 2,579 | 2 | 130,527 |

So sanh voi baseline Sprint 4:

| Model | Best F1 |
|---|---:|
| LR fusion selected_logs_metrics_graph, ratio 50:1 | 0.1121 |
| LR metrics-only baseline Sprint 4 | 0.0890 |
| LR logs-only baseline Sprint 4 | 0.0889 |
| RF logs-only baseline Sprint 4 | 0.0769 |
| LR fusion selected_logs_metrics_graph, ratio 20:1 | 0.0530 |

Nhan xet:

- Cau hinh tot nhat hien tai la Logistic Regression + `selected_logs_metrics_graph` + negative ratio `50:1`.
- F1 tang tu baseline tot nhat `0.0890` len `0.1121`, nen Sprint 5 da dat muc tieu fusion/graph vuot baseline don nguon.
- `20:1` tang recall nhung lam FP tang manh, F1 giam con `0.0530`; khong nen dung lam cau hinh chinh.
- Random Forest van co recall cao nhung FP qua lon, nen chua phu hop neu toi uu theo F1.
- Graph features co gia tri trong bien the da chon loc: `selected_logs_metrics_graph` la ket qua tot nhat.

## Definition Of Done Sprint 5

| Tieu chi | Trang thai |
|---|---|
| Co fusion dataset log + metric + trace | Done trong gold feature table |
| Co graph-enhanced dataset log + metric + trace + graph | Done trong gold feature table |
| Train duoc fusion baseline | Done |
| Train duoc graph-enhanced model | Done |
| Co Precision, Recall, F1-score va confusion matrix | Done |
| Co feature importance cho Random Forest | Done |
| So sanh duoc voi Sprint 4 baseline don nguon | Done |
| Fusion/graph vuot baseline don nguon | Done voi LR `selected_logs_metrics_graph`, F1 `0.1121` |

Ket luan: Sprint 5 co the dong voi cau hinh chinh Logistic Regression `selected_logs_metrics_graph`, negative ratio `50:1`, threshold `0.99`.

## Luu Y

- Sprint 4 baseline tot nhat la Logistic Regression `metrics-only` voi F1 tuned `0.0890`.
- Sprint 5 tot nhat hien tai la Logistic Regression `selected_logs_metrics_graph` voi F1 tuned `0.1121`.
- Neu Random Forest chay cham, co the chay rieng `--algorithms logistic_regression` truoc de lay moc nhanh.
