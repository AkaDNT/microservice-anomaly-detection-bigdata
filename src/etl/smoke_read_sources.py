import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("train-ticket-smoke-read")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def find_first(root: Path, pattern: str) -> Path:
    matches = sorted(root.rglob(pattern))
    if not matches:
        raise FileNotFoundError(f"No file found for pattern {pattern} under {root}")
    return matches[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-read Train-Ticket logs, metrics, and traces with Spark.")
    parser.add_argument("--raw-root", default="data/raw/train-ticket", help="Path to Train-Ticket raw dataset.")
    args = parser.parse_args()

    raw_root = Path(args.raw_root)
    spark = build_spark()

    log_file = find_first(raw_root, "LOGS_*_structured.csv")
    metric_file = find_first(raw_root, "*container_cpu_usage_seconds_total.json")
    trace_file = find_first(raw_root, "*.json")

    logs = spark.read.option("header", True).option("multiLine", False).csv(str(log_file))
    metrics = spark.read.option("multiLine", True).json(str(metric_file))
    traces = spark.read.option("multiLine", True).json(str(trace_file))

    print("=== Smoke Read Summary ===")
    print(f"log_file={log_file}")
    print(f"metric_file={metric_file}")
    print(f"trace_file={trace_file}")
    print(f"log_rows={logs.count()}")
    print(f"metric_root_rows={metrics.count()}")
    print(f"trace_root_rows={traces.count()}")

    if "data" in traces.columns:
        trace_count = traces.select(F.explode("data").alias("trace")).count()
        print(f"trace_records={trace_count}")

    logs.select("Date", "Time", "Level", "EventId", "EventTemplate").show(5, truncate=80)
    spark.stop()


if __name__ == "__main__":
    main()
