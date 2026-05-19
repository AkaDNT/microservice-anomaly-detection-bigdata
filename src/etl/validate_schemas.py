import argparse
from pathlib import Path
from typing import Dict, List

from pyspark.sql import SparkSession


SILVER_REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "logs": ["case_id", "service_name", "timestamp", "level", "event_id", "event_template"],
    "metrics": ["case_id", "service_name", "timestamp", "metric_name", "value"],
    "spans": ["case_id", "service_name", "trace_id", "span_id", "operation_name", "timestamp", "duration_ms"],
    "trace_edges": ["case_id", "trace_id", "source_service", "target_service", "timestamp", "duration_ms"],
    "anomalies": ["case_id", "service_name", "anomaly_timestamp", "raw_text"],
}

GOLD_REQUIRED_COLUMNS = [
    "case_id",
    "service_name",
    "window_start",
    "window_end",
    "label",
    "log_count",
    "error_count",
    "cpu_mean",
    "memory_mean",
    "span_count",
    "avg_duration_ms",
    "in_degree",
    "weighted_call_count",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("train-ticket-validate-schemas")
        .master("local[2]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def validate_required_columns(spark: SparkSession, path: Path, table_name: str, required_columns: List[str]) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing table {table_name}: {path}")

    df = spark.read.parquet(str(path))
    columns = set(df.columns)
    missing = [column for column in required_columns if column not in columns]
    if missing:
        raise ValueError(f"{table_name} missing required columns: {missing}")

    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"{table_name} has zero rows")

    print(f"{table_name}: schema ok, rows={row_count}, columns={len(df.columns)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate required schemas for silver and gold tables.")
    parser.add_argument("--silver-root", default="data_lake/silver")
    parser.add_argument("--gold-root", default="data_lake/gold")
    parser.add_argument("--gold-table", default="window_features")
    parser.add_argument("--silver-tables", default="logs,metrics,spans,trace_edges,anomalies")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver_root = Path(args.silver_root)
    for table in [item.strip() for item in args.silver_tables.split(",") if item.strip()]:
        if table not in SILVER_REQUIRED_COLUMNS:
            raise ValueError(f"Unsupported silver table for schema validation: {table}")
        validate_required_columns(spark, silver_root / table, f"silver.{table}", SILVER_REQUIRED_COLUMNS[table])

    validate_required_columns(
        spark,
        Path(args.gold_root) / args.gold_table,
        f"gold.{args.gold_table}",
        GOLD_REQUIRED_COLUMNS,
    )

    spark.stop()


if __name__ == "__main__":
    main()
