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

Ket luan: Sprint 6 da Done. Co the dung `reports/models/pipeline.log` va `reports/dashboard/dashboard_summary.md` lam bang chung cho bao cao/demo.
