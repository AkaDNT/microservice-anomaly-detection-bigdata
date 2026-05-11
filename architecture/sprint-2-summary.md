# Sprint 2 Summary - Bronze To Silver ETL

## Trang Thai

Trang thai: Hoan thanh runtime validation trong WSL.

Sprint 2 tap trung bien raw Train-Ticket telemetry thanh cac bang silver Parquet co schema ro rang:

- `silver_logs`
- `silver_metrics`
- `silver_spans`
- `silver_trace_edges`
- `silver_anomalies`

## Runtime Rule

Tiep tuc ap dung rule da chot o Sprint 1:

- Sua code bang IDE tren Windows.
- Chay Java, Python, Spark va Airflow trong WSL Ubuntu.
- Project path trong WSL: `/mnt/d/projects/big-data`.
- Script runtime uu tien `.sh`.

## Artifact Da Tao

Code ETL:

- `src/etl/build_silver.py`
- `src/etl/validate_silver.py`

Scripts WSL:

- `scripts/run_silver_etl.sh`
- `scripts/validate_silver.sh`

Tai lieu cap nhat:

- `src/etl/README.md`
- `architecture/sprint-2-summary.md`

## Bang Silver Du Kien

### 1. `data_lake/silver/logs`

Nguon:

```text
LOGS_*_structured.csv
```

Cot chinh:

| Cot | Mo ta |
|---|---|
| `case_id` | Case folder |
| `service_name` | Service rut gon, vi du `ts-order-service` |
| `service_full_name` | Ten service kem version/config |
| `timestamp` | Timestamp da chuan hoa |
| `line_id` | ID dong log |
| `level` | INFO/WARN/ERROR |
| `event_id` | Event template hash |
| `event_template` | Template log |
| `content` | Noi dung log |
| `is_error` | Flag loi |
| `is_warn` | Flag warning |
| `is_span_reported` | Flag log co span reported |
| `source_file` | File goc |

### 2. `data_lake/silver/metrics`

Nguon:

```text
Monitoring_*/*.json
```

Cot chinh:

| Cot | Mo ta |
|---|---|
| `case_id` | Case folder |
| `service_name` | Service rut gon tu folder monitoring |
| `service_full_name` | Ten monitoring group day du |
| `metric_name` | Ten Prometheus metric |
| `timestamp` | Timestamp da chuan hoa |
| `timestamp_unix` | Unix timestamp goc |
| `value` | Gia tri metric |
| `container` | Container |
| `pod` | Pod |
| `namespace` | Namespace |
| `node` | Node |
| `instance` | Instance |
| `source_file` | File goc |

Chi giu priority metrics trong `configs/project_config.json`.

### 3. `data_lake/silver/spans`

Nguon:

```text
Traces_*/*.json
```

Cot chinh:

| Cot | Mo ta |
|---|---|
| `case_id` | Case folder |
| `trace_id` | Trace ID |
| `span_id` | Span ID |
| `parent_span_id` | Parent span ID tu references |
| `process_id` | Process ID trong Jaeger |
| `service_name` | Service map tu `processes` |
| `operation_name` | Operation |
| `timestamp` | Start time |
| `start_time_unix_us` | Timestamp microseconds goc |
| `duration_us` | Duration microseconds |
| `duration_ms` | Duration milliseconds |
| `http_status` | HTTP status |
| `http_method` | HTTP method |
| `http_url` | HTTP URL |
| `component` | Component tag |
| `span_kind` | Client/server/internal |
| `is_error` | Error tag hoac HTTP 5xx |
| `source_file` | File goc |

### 4. `data_lake/silver/trace_edges`

Nguon:

```text
silver_spans self-join theo trace_id va parent_span_id
```

Cot chinh:

| Cot | Mo ta |
|---|---|
| `case_id` | Case folder |
| `trace_id` | Trace ID |
| `source_service` | Service cua parent span |
| `target_service` | Service cua child span |
| `operation_name` | Operation cua child span |
| `timestamp` | Timestamp child span |
| `duration_us` | Duration child span |
| `duration_ms` | Duration child span |
| `http_status` | HTTP status |
| `http_method` | HTTP method |
| `is_error` | Edge co loi |
| `source_file` | File goc |

### 5. `data_lake/silver/anomalies`

Nguon:

```text
potentialAnomalies_*.txt
```

Cot chinh:

| Cot | Mo ta |
|---|---|
| `case_id` | Case folder |
| `service_name` | Service rut gon |
| `service_full_name` | Service kem version/config |
| `anomaly_timestamp` | Timestamp anomaly trich xuat bang regex |
| `raw_text` | Dong text goc |
| `source_file` | File goc |

## Cach Chay Trong WSL

Di chuyen vao project:

```bash
cd /mnt/d/projects/big-data
```

Chay debug voi case nho truoc:

```bash
bash scripts/run_silver_etl.sh logs "case_07_order_mongodb_4_2_2_20220712"
bash scripts/run_silver_etl.sh anomalies "case_07_order_mongodb_4_2_2_20220712"
```

Chay all source tren 2 case co du metrics/traces:

```bash
bash scripts/run_silver_etl.sh all "case_07_order_mongodb_4_2_2_20220712,case_10_order_springdata_mongodb_2_0_0_20220711"
```

Chay toan bo dataset:

```bash
bash scripts/run_silver_etl.sh all
```

Validate outputs:

```bash
bash scripts/validate_silver.sh
```

Log chay ETL duoc luu tai:

```text
reports/silver/build_silver_<source>_<timestamp>.log
reports/silver/build_silver_<source>.log
reports/silver/validate_silver_<timestamp>.log
reports/silver/validate_silver.log
```

## Luu Y Khi Chay

- Mot so case thieu traces hoac metrics theo inventory Sprint 1.
- ETL phai chap nhan missing modality, khong duoc gia dinh case nao cung co du 3 nguon.
- Nen chay subset truoc, sau do moi chay all.
- Silver output dang Parquet va bi `.gitignore`, khong commit len Git.

## Definition Of Done Sprint 2

Sprint 2 dat DoD khi:

- `bash scripts/run_silver_etl.sh logs` chay thanh cong.
- `bash scripts/run_silver_etl.sh metrics` chay thanh cong voi case co metrics.
- `bash scripts/run_silver_etl.sh traces` chay thanh cong voi case co traces.
- `bash scripts/run_silver_etl.sh anomalies` chay thanh cong.
- Co cac folder Parquet:
  - `data_lake/silver/logs`
  - `data_lake/silver/metrics`
  - `data_lake/silver/spans`
  - `data_lake/silver/trace_edges`
  - `data_lake/silver/anomalies`
- `bash scripts/validate_silver.sh` in duoc row count va case count cho cac bang silver.
- Logs chay ETL/validation nam trong `reports/silver`.

## Ket Luan

Sprint 2 da hoan thanh. `bash scripts/run_silver_etl.sh all` va `bash scripts/validate_silver.sh` da chay thanh cong trong WSL.

Ket qua validation luc `2026-05-11 23:21:29 +0700`:

| Bang | Rows | Cases | Ghi chu |
|---|---:|---:|---|
| `logs` | 1,148,240 | 10 | Du 10 case |
| `metrics` | 12,684,274 | 9 | Case 04 khong co monitoring priority JSON |
| `spans` | 219,252 | 9 | Case 04 khong co Jaeger trace JSON; `MicroRCA` la metrics/RCA CSV, khong phai trace |
| `trace_edges` | 2,919,729 | 9 | Tao tu parent-child spans |
| `anomalies` | 103 | 8 | Case 03 va 08 ghi `no anomalies identified`, khong co timestamp event |

Co the chuyen sang Sprint 3: Gold Layer, Windowing, Labels va Feature Engineering.
