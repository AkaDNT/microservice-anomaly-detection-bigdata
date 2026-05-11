# Sprint 1 Setup Guide

## Runtime Rule

**Rule chot:** code co the edit tren Windows, nhung cac runtime Big Data nen chay trong WSL Ubuntu de on dinh hon.

Duong dan lam viec:

```text
Windows: D:\projects\big-data
WSL:     /mnt/d/projects/big-data
```

Quy uoc:

- Dung Windows/IDE de doc va sua file.
- Dung WSL de chay Java, Python, Spark, Airflow va cac shell script.
- Khong hard-code path Windows trong Spark jobs.
- Tat ca script moi nen co ban `.sh` cho WSL.
- PowerShell scripts chi dung nhu tien ich phu khi can scan nhanh tren Windows.

## Current Environment Check

The repository structure and inventory scripts are ready. Runtime validation should be performed inside WSL. WSL needs these tools before Spark jobs can run:

- Java JDK.
- Python.
- Apache Spark with PySpark.

Checks that need to pass inside WSL:

```bash
java -version
python3 --version
spark-submit --version
```

## Recommended WSL Setup

Install:

- Java JDK 11 or 17.
- Python 3.10 or 3.11.
- Apache Spark 3.5.x pre-built for Hadoop 3.

Example WSL commands:

```bash
cd /mnt/d/projects/big-data
sudo apt update
sudo apt install -y openjdk-17-jdk python3 python3-pip ython3-venv
```

Install Python dependencies:

```bash
python3 -m venv .venv
pip3 install -r requirements.txt
```

## Run Inventory

Recommended in WSL:

```bash
bash scripts/scan_dataset.sh
```

Outputs:

```text
reports/inventory/dataset_inventory.csv
reports/inventory/dataset_inventory.md
```

Windows fallback:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\scan_dataset.ps1
```

## Run Spark Smoke Test

Recommended in WSL:

```bash
bash scripts/run_smoke_read.sh
```

Outputs:

```text
reports/smoke/smoke_read_sources_<timestamp>.log
reports/smoke/smoke_read_sources.log
```

Expected result:

- Prints selected sample log, metric and trace files.
- Prints row counts.
- Shows 5 sample structured log rows.

## Sprint 1 Done Criteria

Sprint 1 runtime validation is complete when:

- `java -version` works inside WSL.
- `python3 --version` works inside WSL.
- `spark-submit --version` works inside WSL.
- `bash scripts/run_smoke_read.sh` completes successfully and writes logs to `reports/smoke`.
