# Sprint 3 Summary - Gold Layer, Windowing, Labels Va Feature Engineering

## Trang Thai

Trang thai: Hoan thanh Sprint 3; Gold ETL va validation da chay thanh cong trong WSL.

Sprint 3 bien cac bang silver thanh bang feature window-level phuc vu modeling:

```text
data_lake/gold/window_features
```

Khoa logic:

```text
case_id, service_name, window_start, window_end
```

Window mac dinh:

```text
60 seconds
```

## Artifact Da Tao

Code ETL:

- `src/etl/build_gold.py`
- `src/etl/validate_gold.py`

Scripts WSL:

- `scripts/run_gold_etl.sh`
- `scripts/validate_gold.sh`

Tai lieu cap nhat:

- `src/etl/README.md`
- `architecture/sprint-2-summary.md`
- `architecture/sprint-3-summary.md`

## Feature Groups

### Log features

- `log_count`
- `error_count`
- `warn_count`
- `info_count`
- `unique_event_id_count`
- `span_reported_count`
- `top_event_frequency`
- `template_entropy`

### Metric features

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

Metrics duoc group theo `container` neu co, fallback sang `pod`, roi fallback sang monitoring `service_name`.

### Trace features

- `trace_count`
- `span_count`
- `avg_duration_ms`
- `max_duration_ms`
- `p95_duration_ms`
- `error_span_count`
- `http_4xx_count`
- `http_5xx_count`
- `unique_operation_count`

### Graph features

- `in_degree`
- `out_degree`
- `weighted_call_count`
- `avg_edge_latency_ms`
- `max_edge_latency_ms`
- `error_edge_count`
- `unique_peer_service_count`

Graph features duoc tinh lightweight bang Spark SQL aggregation tren `silver/trace_edges`.

### Label

- `label`

Label hien tai la relaxed label theo `case_id`, `service_name`, va timestamp anomaly.
Gold gan `label = 1` khi `anomaly_timestamp` nam trong khoang:

```text
window_start - relaxed_label_seconds <= anomaly_timestamp <= window_end + relaxed_label_seconds
```

Gia tri mac dinh cua `relaxed_label_seconds` la 120 giay.
Service label duoc lay tu `silver/anomalies.service_name`, trong do silver uu tien service suy luan tu raw anomaly text; neu anomaly co `trace_id`, gold cung label cac service xuat hien tren cung trace.

## Cach Chay Trong WSL

```bash
cd /mnt/d/projects/big-data
bash scripts/run_gold_etl.sh
bash scripts/validate_gold.sh
```

Thu window size khac:

```bash
bash scripts/run_gold_etl.sh 30
bash scripts/run_gold_etl.sh 120
```

Log chay ETL/validation:

```text
reports/gold/build_gold_<window>s_<timestamp>.log
reports/gold/build_gold.log
reports/gold/validate_gold_<timestamp>.log
reports/gold/validate_gold.log
```

## Definition Of Done Sprint 3

| Tieu chi | Trang thai |
|---|---|
| `bash scripts/run_gold_etl.sh` chay thanh cong | Done |
| Co folder Parquet `data_lake/gold/window_features` | Done |
| `bash scripts/validate_gold.sh` in duoc row count, case count va label distribution | Done voi lan validate cu; can rerun de cap nhat log moi |
| Bang gold co du key `case_id`, `service_name`, `window_start`, `window_end` | Done |
| Bang gold co `label` va cac feature log/metric/trace/graph co ban | Done |

Ket luan: **Sprint 3 Done**.

## Ket Qua Rebuild Label Relaxed 120s

Gold ETL rebuild luc `2026-05-19 10:45:30 +0700` voi:

```text
window_seconds=60
relaxed_label_seconds=120
```

Ket qua build trong `reports/gold/build_gold.log`:

| Metric | Gia tri |
|---|---:|
| Tong rows | 401,806 |
| Label 0 | 401,615 |
| Label 1 | 191 |

So label 1 tang tu 40 len 191 sau khi:

- Noi buffer label len 120 giay quanh moi window.
- Uu tien service suy luan tu raw anomaly text.
- Bo sung label cho service xuat hien tren cung trace id khi anomaly co `trace_id`.

## Luu Y

- Mot so case thieu modality la binh thuong: case 04 khong co Jaeger trace JSON; case 03 va 08 khong co anomaly timestamp vi file ghi `no anomalies identified`.
- Label hien tai la baseline de tien hanh Sprint 4/5; co the so sanh strict label, relaxed 60s va relaxed 120s neu can ablation.
- `reports/gold/validate_gold.log` hien van la log cu ngay `2026-05-12`; ket qua label moi duoc xac nhan trong `reports/gold/build_gold.log`.
