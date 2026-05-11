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

Label hien tai la relaxed label theo `case_id`, `service_name`, va `anomaly_timestamp +- 60s`.

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

Sprint 3 dat DoD khi:

- `bash scripts/run_gold_etl.sh` chay thanh cong.
- Co folder Parquet `data_lake/gold/window_features`.
- `bash scripts/validate_gold.sh` in duoc row count, case count va label distribution.
- Bang gold co du key `case_id`, `service_name`, `window_start`, `window_end`.
- Bang gold co `label` va cac feature log/metric/trace/graph co ban.

## Luu Y

- Mot so case thieu modality la binh thuong: case 04 khong co Jaeger trace JSON; case 03 va 08 khong co anomaly timestamp vi file ghi `no anomalies identified`.
- Label hien tai la baseline de tien hanh Sprint 4/5; co the thu strict label hoac relaxed +-120s neu can so sanh.
