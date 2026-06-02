import argparse
import json
from pathlib import Path
from typing import Iterable, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "train-ticket-build-silver", timezone: str = "Asia/Shanghai") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.files.maxPartitionBytes", "16m")
        .config("spark.sql.session.timeZone", timezone)
        .getOrCreate()
    )


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def list_files(raw_root: Path, cases: Iterable[str], pattern: str) -> List[str]:
    files: List[str] = []
    for case_id in cases:
        case_dir = raw_root / case_id
        if case_dir.exists():
            files.extend(str(path) for path in sorted(case_dir.rglob(pattern)))
    return files


def discover_cases(raw_root: Path, selected_cases: str) -> List[str]:
    if selected_cases:
        return [case.strip() for case in selected_cases.split(",") if case.strip()]
    return sorted(path.name for path in raw_root.glob("case_*") if path.is_dir())


def with_path_columns(df: DataFrame) -> DataFrame:
    source_file = F.input_file_name()
    return (
        df.withColumn("source_file", source_file)
        .withColumn("case_id", F.regexp_extract("source_file", r"(case_[^/\\]+)", 1))
    )


def write_parquet(df: DataFrame, output_path: str, partition_cols: List[str], mode: str = "overwrite") -> None:
    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(output_path)


def struct_field_or_null(df: DataFrame, struct_col: str, field_name: str) -> F.Column:
    try:
        data_type = df.schema[struct_col].dataType
    except KeyError:
        return F.lit(None).cast("string")

    if isinstance(data_type, T.StructType) and field_name in data_type.names:
        return F.col(f"{struct_col}.{field_name}").cast("string")
    return F.lit(None).cast("string")


def is_priority_metric_file(path: Path, priority_metrics: List[str]) -> bool:
    if not priority_metrics:
        return True
    return any(path.name.endswith(f"{metric_name}.json") for metric_name in priority_metrics)


def list_trace_files(case_dir: Path) -> List[str]:
    files: List[Path] = []
    for child in sorted(path for path in case_dir.iterdir() if path.is_dir()):
        if child.name.startswith("Monitoring_") or child.name == "MicroRCA":
            continue
        if child.name.startswith(("Trace_", "Traces_")) or "_2022-" in child.name:
            files.extend(sorted(child.rglob("*.json")))
    return [str(path) for path in files]


def build_logs(spark: SparkSession, raw_root: Path, cases: List[str], silver_root: str) -> None:
    files = list_files(raw_root, cases, "LOGS_*_structured.csv")
    if not files:
        print("No structured log files found.")
        return

    logs = spark.read.option("header", True).option("multiLine", False).csv(files)
    logs = with_path_columns(logs)

    logs = (
        logs.withColumn("service_full_name", F.regexp_extract("source_file", r"LOGS_(.+)\.txt_structured\.csv$", 1))
        .withColumn("service_name", F.split("service_full_name", "_").getItem(0))
        .withColumn("timestamp", F.to_timestamp(F.concat_ws(" ", F.col("Date"), F.col("Time")), "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("line_id", F.col("LineId").cast("long"))
        .withColumn("level", F.upper(F.trim(F.col("Level"))))
        .withColumn("event_id", F.col("EventId"))
        .withColumn("event_template", F.col("EventTemplate"))
        .withColumn("content", F.col("Content"))
        .withColumn("is_error", (F.col("level") == "ERROR") | F.lower(F.col("content")).contains("error"))
        .withColumn("is_warn", F.col("level") == "WARN")
        .withColumn("is_span_reported", F.col("content").contains("Span reported"))
        .select(
            "case_id",
            "service_name",
            "service_full_name",
            "timestamp",
            "line_id",
            "level",
            "event_id",
            "event_template",
            "content",
            "is_error",
            "is_warn",
            "is_span_reported",
            "source_file",
        )
    )

    output_path = str(Path(silver_root) / "logs")
    write_parquet(logs, output_path, ["case_id"])
    print(f"Wrote silver logs to {output_path}")


def build_metrics(spark: SparkSession, raw_root: Path, cases: List[str], silver_root: str, priority_metrics: List[str]) -> None:
    output_path = str(Path(silver_root) / "metrics")
    wrote_any = False

    for case_id in cases:
        case_dir = raw_root / case_id
        if not case_dir.exists():
            continue

        files = [
            str(path)
            for path in sorted(case_dir.glob("Monitoring_*/*.json"))
            if is_priority_metric_file(path, priority_metrics)
        ]
        if not files:
            print(f"No priority monitoring JSON files found for {case_id}.")
            continue

        raw = spark.read.option("multiLine", True).json(files)
        raw = with_path_columns(raw)

        metrics = (
            raw.select("case_id", "source_file", F.explode_outer("data.result").alias("result"))
            .withColumn("metric", F.col("result.metric"))
            .withColumn("point", F.explode_outer("result.values"))
        )

        metrics = (
            metrics.withColumn("metric_name", struct_field_or_null(metrics, "metric", "__name__"))
            .withColumn("service_full_name", F.regexp_extract("source_file", r"Monitoring_(.+)\.json_[^/\\]+", 1))
            .withColumn("service_name", F.split("service_full_name", "_").getItem(0))
            .withColumn("timestamp_unix", F.col("point").getItem(0).cast("double"))
            .withColumn("timestamp", F.to_timestamp(F.from_unixtime(F.col("timestamp_unix").cast("long"))))
            .withColumn("value", F.col("point").getItem(1).cast("double"))
            .withColumn(
                "container",
                F.coalesce(
                    struct_field_or_null(metrics, "metric", "container"),
                    struct_field_or_null(metrics, "metric", "container_name"),
                ),
            )
            .withColumn(
                "pod",
                F.coalesce(
                    struct_field_or_null(metrics, "metric", "pod"),
                    struct_field_or_null(metrics, "metric", "pod_name"),
                ),
            )
            .withColumn("namespace", struct_field_or_null(metrics, "metric", "namespace"))
            .withColumn("node", struct_field_or_null(metrics, "metric", "node"))
            .withColumn("instance", struct_field_or_null(metrics, "metric", "instance"))
        )

        if priority_metrics:
            metrics = metrics.where(F.col("metric_name").isin(priority_metrics))

        metrics = metrics.select(
            "case_id",
            "service_name",
            "service_full_name",
            "metric_name",
            "timestamp",
            "timestamp_unix",
            "value",
            "container",
            "pod",
            "namespace",
            "node",
            "instance",
            "source_file",
        )

        mode = "overwrite" if not wrote_any else "append"
        write_parquet(metrics, output_path, ["case_id", "metric_name"], mode=mode)
        wrote_any = True
        print(f"Wrote silver metrics for {case_id} to {output_path}")

    if not wrote_any:
        print("No monitoring JSON files found.")
        return

    print(f"Wrote silver metrics to {output_path}")


def tag_value(key: str) -> F.Column:
    return F.expr(f"element_at(transform(filter(span.tags, x -> x.key = '{key}'), x -> cast(x.value as string)), 1)")


def build_traces(spark: SparkSession, raw_root: Path, cases: List[str], silver_root: str) -> None:
    process_value_schema = T.StructType(
        [
            T.StructField("serviceName", T.StringType(), True),
            T.StructField(
                "tags",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("key", T.StringType(), True),
                            T.StructField("type", T.StringType(), True),
                            T.StructField("value", T.StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    process_map_schema = T.MapType(T.StringType(), process_value_schema)

    spans_output = str(Path(silver_root) / "spans")
    edges_output = str(Path(silver_root) / "trace_edges")
    wrote_any = False

    for case_id in cases:
        case_dir = raw_root / case_id
        if not case_dir.exists():
            continue

        files = list_trace_files(case_dir)
        if not files:
            print(f"No trace JSON files found for {case_id}.")
            continue

        raw = spark.read.option("multiLine", True).json(files)
        raw = with_path_columns(raw)

        traces = raw.select("case_id", "source_file", F.explode_outer("data").alias("trace"))
        traces = traces.withColumn("processes_map", F.from_json(F.to_json("trace.processes"), process_map_schema))

        spans = (
            traces.select("case_id", "source_file", "processes_map", F.explode_outer("trace.spans").alias("span"))
            .withColumn("trace_id", F.col("span.traceID"))
            .withColumn("span_id", F.col("span.spanID"))
            .withColumn("parent_span_id", F.expr("element_at(transform(span.references, x -> x.spanID), 1)"))
            .withColumn("process_id", F.col("span.processID"))
            .withColumn("process_info", F.element_at(F.col("processes_map"), F.col("process_id")))
            .withColumn("service_name", F.col("process_info.serviceName"))
            .withColumn("operation_name", F.col("span.operationName"))
            .withColumn("start_time_unix_us", F.col("span.startTime").cast("long"))
            .withColumn(
                "timestamp",
                F.to_timestamp(F.from_unixtime((F.col("start_time_unix_us") / F.lit(1000000)).cast("long"))),
            )
            .withColumn("duration_us", F.col("span.duration").cast("long"))
            .withColumn("duration_ms", F.col("duration_us") / F.lit(1000.0))
            .withColumn("http_status", tag_value("http.status_code").cast("int"))
            .withColumn("http_method", tag_value("http.method"))
            .withColumn("http_url", tag_value("http.url"))
            .withColumn("component", tag_value("component"))
            .withColumn("span_kind", tag_value("span.kind"))
            .withColumn("error_tag", tag_value("error"))
            .withColumn("is_error", (F.lower(F.col("error_tag")) == "true") | (F.col("http_status") >= 500))
            .select(
                "case_id",
                "trace_id",
                "span_id",
                "parent_span_id",
                "process_id",
                "service_name",
                "operation_name",
                "timestamp",
                "start_time_unix_us",
                "duration_us",
                "duration_ms",
                "http_status",
                "http_method",
                "http_url",
                "component",
                "span_kind",
                "is_error",
                "source_file",
            )
        )

        mode = "overwrite" if not wrote_any else "append"
        write_parquet(spans, spans_output, ["case_id"], mode=mode)
        print(f"Wrote silver spans for {case_id} to {spans_output}")

        parent = spans.select(
            F.col("case_id").alias("parent_case_id"),
            F.col("trace_id").alias("parent_trace_id"),
            F.col("span_id").alias("parent_id"),
            F.col("service_name").alias("source_service"),
        )

        edges = (
            spans.alias("child")
            .join(
                parent,
                (F.col("child.case_id") == F.col("parent_case_id"))
                & (F.col("child.trace_id") == F.col("parent_trace_id"))
                & (F.col("child.parent_span_id") == F.col("parent_id")),
                "left",
            )
            .where(F.col("child.parent_span_id").isNotNull())
            .select(
                F.col("child.case_id").alias("case_id"),
                F.col("child.trace_id").alias("trace_id"),
                F.col("source_service"),
                F.col("child.service_name").alias("target_service"),
                F.col("child.operation_name"),
                F.col("child.timestamp"),
                F.col("child.duration_us"),
                F.col("child.duration_ms"),
                F.col("child.http_status"),
                F.col("child.http_method"),
                F.col("child.is_error"),
                F.col("child.source_file"),
            )
        )

        write_parquet(edges, edges_output, ["case_id"], mode=mode)
        wrote_any = True
        print(f"Wrote silver trace edges for {case_id} to {edges_output}")

    if not wrote_any:
        print("No trace JSON files found.")
        return

    print(f"Wrote silver spans to {spans_output}")
    print(f"Wrote silver trace edges to {edges_output}")


def build_anomalies(spark: SparkSession, raw_root: Path, cases: List[str], silver_root: str) -> None:
    files = list_files(raw_root, cases, "potentialAnomalies_*.txt")
    if not files:
        print("No anomaly files found.")
        return

    anomalies = spark.read.text(files)
    anomalies = with_path_columns(anomalies)

    raw_lower = F.lower(F.col("value"))
    inferred_service = (
        F.when(raw_lower.contains("admin basic info") | raw_lower.contains("adminbasic"), F.lit("ts-admin-basic-info-service"))
        .when(raw_lower.contains("admin order") | raw_lower.contains("adminorder") | raw_lower.contains("ts-admin-order-service"), F.lit("ts-admin-order-service"))
        .when(raw_lower.contains("admin travel") | raw_lower.contains("admintravel"), F.lit("ts-admin-travel-service"))
        .when(raw_lower.contains("admin user") | raw_lower.contains("adminuser"), F.lit("ts-admin-user-service"))
        .when(raw_lower.contains("food map") | raw_lower.contains("foodmap") | raw_lower.contains("food-map"), F.lit("ts-food-map-service"))
        .when(raw_lower.contains("food service") | raw_lower.contains("food-service") | raw_lower.contains("foodsearch"), F.lit("ts-food-service"))
        .when(raw_lower.contains("preserve other service") | raw_lower.contains("preserve other"), F.lit("ts-preserve-other-service"))
        .when(raw_lower.contains("preserve service") | raw_lower.contains("preserve."), F.lit("ts-preserve-service"))
        .when(raw_lower.contains("order other service") | raw_lower.contains("orderother"), F.lit("ts-order-other-service"))
        .when(raw_lower.contains("order service") | raw_lower.contains("order."), F.lit("ts-order-service"))
        .when(raw_lower.contains("travel2") | raw_lower.contains("travel 2"), F.lit("ts-travel2-service"))
        .when(raw_lower.contains("travel service") | raw_lower.contains("travel."), F.lit("ts-travel-service"))
        .when(raw_lower.contains("route service") | raw_lower.contains("route."), F.lit("ts-route-service"))
        .when(raw_lower.contains("contacts service") | raw_lower.contains("contacts."), F.lit("ts-contacts-service"))
        .when(raw_lower.contains("basic service") | raw_lower.contains("basic."), F.lit("ts-basic-service"))
        .when(raw_lower.contains("station service") | raw_lower.contains("station."), F.lit("ts-station-service"))
        .when(raw_lower.contains("ticketinfo service") | raw_lower.contains("ticketinfo."), F.lit("ts-ticketinfo-service"))
        .when(raw_lower.contains("train service") | raw_lower.contains("train."), F.lit("ts-train-service"))
    )

    anomalies = (
        anomalies.withColumn("service_full_name", F.regexp_extract("source_file", r"potentialAnomalies_(.+)\.txt$", 1))
        .withColumn("source_service_name", F.split("service_full_name", "_").getItem(0))
        .withColumn("timestamp_text", F.regexp_extract("value", r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})", 1))
        .where(F.col("timestamp_text") != "")
        .withColumn("anomaly_timestamp", F.to_timestamp("timestamp_text", "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("inferred_service_name", inferred_service)
        .withColumn("service_name", F.coalesce(F.col("inferred_service_name"), F.col("source_service_name")))
        .withColumn("trace_id", F.regexp_extract("value", r"Span reported:\s*([0-9a-fA-F]+):", 1))
        .withColumn("trace_id", F.when(F.col("trace_id") != "", F.col("trace_id")))
        .withColumn("raw_text", F.col("value"))
        .select(
            "case_id",
            "service_name",
            "source_service_name",
            "inferred_service_name",
            "service_full_name",
            "trace_id",
            "anomaly_timestamp",
            "raw_text",
            "source_file",
        )
    )

    output_path = str(Path(silver_root) / "anomalies")
    write_parquet(anomalies, output_path, ["case_id"])
    print(f"Wrote silver anomalies to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build silver layer tables from Train-Ticket raw telemetry.")
    parser.add_argument("--config", default="configs/project_config.json")
    parser.add_argument("--raw-root", default=None)
    parser.add_argument("--silver-root", default=None)
    parser.add_argument("--cases", default="", help="Comma-separated case folder names. Empty means all cases.")
    parser.add_argument(
        "--source",
        default="all",
        choices=["all", "logs", "metrics", "traces", "anomalies"],
        help="Silver source to build.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    raw_root = Path(args.raw_root or config["raw_data_root"])
    silver_root = args.silver_root or config["silver_root"]
    cases = discover_cases(raw_root, args.cases)

    print(f"raw_root={raw_root}")
    print(f"silver_root={silver_root}")
    print(f"cases={cases}")
    print(f"source={args.source}")

    spark = build_spark(timezone=config.get("dataset_timezone", "Asia/Shanghai"))
    spark.sparkContext.setLogLevel("WARN")

    if args.source in ("all", "logs"):
        build_logs(spark, raw_root, cases, silver_root)
    if args.source in ("all", "metrics"):
        build_metrics(spark, raw_root, cases, silver_root, config.get("priority_metrics", []))
    if args.source in ("all", "traces"):
        build_traces(spark, raw_root, cases, silver_root)
    if args.source in ("all", "anomalies"):
        build_anomalies(spark, raw_root, cases, silver_root)

    spark.stop()


if __name__ == "__main__":
    main()
