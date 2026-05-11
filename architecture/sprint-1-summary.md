# Sprint 1 Summary - Thiet Lap Moi Truong Va Data Lake

## Trang Thai

Trang thai: Hoan thanh phan repository va data inventory; runtime Spark se validate trong WSL.

Sprint 1 da hoan thanh phan project structure, local data lake layout, config, script inventory va Spark smoke test script. Rule moi: edit code tren Windows/IDE, chay Java, Python, Spark va Airflow trong WSL Ubuntu tai `/mnt/d/projects/big-data`.

## Da Hoan Thanh

- Tao cau truc thu muc project:
  - `src/etl`
  - `src/features`
  - `src/models`
  - `src/utils`
  - `airflow/dags`
  - `notebooks`
  - `configs`
  - `scripts`
  - `reports/inventory`
  - `reports/metrics`
- Tao local data lake layout:
  - `data_lake/bronze/train-ticket`
  - `data_lake/silver/logs`
  - `data_lake/silver/metrics`
  - `data_lake/silver/spans`
  - `data_lake/silver/trace_edges`
  - `data_lake/silver/anomalies`
  - `data_lake/gold/window_features`
- Tao config chung:
  - `configs/project_config.json`
- Tao script scan dataset:
  - `scripts/scan_dataset.ps1`
- Tao Spark smoke test:
  - `src/etl/smoke_read_sources.py`
- Tao script WSL/Linux:
  - `scripts/scan_dataset.sh`
  - `scripts/run_smoke_read.sh`
- Tao Python dependency file:
  - `requirements.txt`
- Tao README cho data lake va ETL jobs.
- Da chay inventory thanh cong:
  - `reports/inventory/dataset_inventory.csv`
  - `reports/inventory/dataset_inventory.md`

## Ket Qua Inventory

Ket qua scan luc `2026-05-11 08:48:43 +07:00`:

| Metric | Value |
|---|---:|
| Cases | 10 |
| Raw files | 552 |
| Structured log rows | 1,148,240 |
| Approx size MB | 1,398.75 |

Ghi chu quan trong:

- Tat ca 10 case deu co structured logs va anomaly file.
- Mot so case khong co trace JSON theo scan hien tai:
  - `case_03_auth_mongo_5_0_9_20220706`
  - `case_04_auth_mongodb_4_4_15_20220727`
  - `case_09_order_springdata_mongodb_1_5_22_20220711`
- Mot so case khong co monitoring JSON theo scan hien tai:
  - `case_04_auth_mongodb_4_4_15_20220727`
- Cac sprint sau can xu ly missing modality thay vi gia dinh case nao cung co du logs, metrics va traces.

## Can Chay De Hoan Tat Sprint 1

Chay inventory trong WSL:

```bash
cd /mnt/d/projects/big-data
bash scripts/scan_dataset.sh
```

Hoac chay inventory tren Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\scan_dataset.ps1
```

Sau khi cai Java, Python va Spark trong WSL, chay smoke test:

```bash
cd /mnt/d/projects/big-data
bash scripts/run_smoke_read.sh
```

Smoke test se ghi log vao:

```text
reports/smoke/smoke_read_sources_<timestamp>.log
reports/smoke/smoke_read_sources.log
```

## Tieu Chi Con Thieu

- `spark-submit` chay duoc trong WSL.
- `java` chay duoc trong WSL.
- Python runtime/PySpark chay duoc trong WSL.
- Smoke test doc thanh cong it nhat:
  - 1 file structured logs CSV.
  - 1 file monitoring metrics JSON.
  - 1 file traces JSON.
- Smoke test ghi log vao `reports/smoke`.
- Co inventory report trong `reports/inventory`.

## Ket Luan

Phan artifact cua Sprint 1 da san sang de tiep tuc Sprint 2. Truoc khi chay Spark ETL that su, can cai dat Java, Python va Apache Spark/PySpark trong WSL, sau do chay `bash scripts/run_smoke_read.sh` de xac nhan moi truong.
