import argparse
import json
from functools import reduce
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


KEY_COLS = ["case_id", "service_name", "window_start", "window_end"]


def build_spark(app_name: str = "train-ticket-build-gold", timezone: str = "Asia/Shanghai") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", timezone)
        .getOrCreate()
    )


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def table_exists(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.parquet"))


def read_silver(spark: SparkSession, silver_root: Path, table: str) -> DataFrame:
    path = silver_root / table
    if not table_exists(path):
        raise FileNotFoundError(f"Missing silver table: {path}")
    return spark.read.parquet(str(path))


def with_window(df: DataFrame, timestamp_col: str, window_seconds: int) -> DataFrame:
    window_col = F.window(F.col(timestamp_col), f"{window_seconds} seconds")
    return (
        df.where(F.col(timestamp_col).isNotNull())
        .withColumn("window", window_col)
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


def build_log_features(spark: SparkSession, silver_root: Path, window_seconds: int) -> DataFrame:
    logs = with_window(read_silver(spark, silver_root, "logs"), "timestamp", window_seconds)

    base = logs.groupBy(KEY_COLS).agg(
        F.count("*").cast("long").alias("log_count"),
        F.sum(F.col("is_error").cast("int")).cast("long").alias("error_count"),
        F.sum(F.col("is_warn").cast("int")).cast("long").alias("warn_count"),
        F.sum((F.col("level") == "INFO").cast("int")).cast("long").alias("info_count"),
        F.countDistinct("event_id").cast("long").alias("unique_event_id_count"),
        F.sum(F.col("is_span_reported").cast("int")).cast("long").alias("span_reported_count"),
    )

    event_counts = logs.groupBy(*(KEY_COLS + ["event_id"])).agg(F.count("*").alias("event_count"))
    top_event = event_counts.groupBy(KEY_COLS).agg(F.max("event_count").cast("long").alias("top_event_frequency"))
    entropy = (
        event_counts.join(base.select(*(KEY_COLS + ["log_count"])), KEY_COLS, "inner")
        .withColumn("p", F.col("event_count") / F.col("log_count"))
        .groupBy(KEY_COLS)
        .agg(F.sum(-F.col("p") * F.log(F.col("p"))).alias("template_entropy"))
    )

    return base.join(top_event, KEY_COLS, "left").join(entropy, KEY_COLS, "left")


def metric_service_col() -> F.Column:
    container = F.when(
        F.col("container").isNotNull() & (F.col("container") != "") & (F.col("container") != "POD"),
        F.col("container"),
    )
    pod = F.when(F.col("pod").isNotNull() & (F.col("pod") != ""), F.col("pod"))
    return F.coalesce(container, pod, F.col("service_name"))


def build_metric_features(spark: SparkSession, silver_root: Path, window_seconds: int) -> DataFrame:
    metrics = read_silver(spark, silver_root, "metrics")
    metrics = (
        metrics.withColumn("service_name", metric_service_col())
        .where(F.col("service_name").isNotNull() & (F.col("service_name") != ""))
    )
    series_cols = [
        "case_id",
        "service_name",
        "metric_name",
        "container",
        "pod",
        "namespace",
        "node",
        "instance",
        "source_file",
    ]
    series_window = Window.partitionBy(*series_cols).orderBy("timestamp_unix")
    metrics = (
        metrics.withColumn("prev_timestamp_unix", F.lag("timestamp_unix").over(series_window))
        .withColumn("prev_value", F.lag("value").over(series_window))
        .withColumn("delta_seconds", F.col("timestamp_unix") - F.col("prev_timestamp_unix"))
        .withColumn("delta_value", F.col("value") - F.col("prev_value"))
        .withColumn(
            "metric_rate",
            F.when(
                (F.col("delta_seconds") > 0) & (F.col("delta_value") >= 0),
                F.col("delta_value") / F.col("delta_seconds"),
            ),
        )
    )
    metrics = with_window(metrics, "timestamp", window_seconds)

    return metrics.groupBy(KEY_COLS).agg(
        F.avg(F.when(F.col("metric_name") == "container_cpu_usage_seconds_total", F.col("value"))).alias("cpu_mean"),
        F.max(F.when(F.col("metric_name") == "container_cpu_usage_seconds_total", F.col("value"))).alias("cpu_max"),
        F.stddev(F.when(F.col("metric_name") == "container_cpu_usage_seconds_total", F.col("value"))).alias("cpu_std"),
        F.avg(F.when(F.col("metric_name") == "container_memory_working_set_bytes", F.col("value"))).alias("memory_mean"),
        F.max(F.when(F.col("metric_name") == "container_memory_working_set_bytes", F.col("value"))).alias("memory_max"),
        F.stddev(F.when(F.col("metric_name") == "container_memory_working_set_bytes", F.col("value"))).alias("memory_std"),
        F.avg(F.when(F.col("metric_name") == "container_network_transmit_packets_total", F.col("value"))).alias("network_mean"),
        F.max(F.when(F.col("metric_name") == "container_network_transmit_packets_total", F.col("value"))).alias("network_max"),
        F.avg(F.when(F.col("metric_name") == "node_memory_MemAvailable_bytes", F.col("value"))).alias("node_memory_available_mean"),
        F.avg(F.when(F.col("metric_name") == "node_memory_MemTotal_bytes", F.col("value"))).alias("node_memory_total_mean"),
        F.avg(F.when(F.col("metric_name") == "container_cpu_usage_seconds_total", F.col("metric_rate"))).alias("cpu_rate_mean"),
    )


def build_trace_features(spark: SparkSession, silver_root: Path, window_seconds: int) -> DataFrame:
    spans = with_window(read_silver(spark, silver_root, "spans"), "timestamp", window_seconds)
    return spans.groupBy(KEY_COLS).agg(
        F.countDistinct("trace_id").cast("long").alias("trace_count"),
        F.count("*").cast("long").alias("span_count"),
        F.avg("duration_ms").alias("avg_duration_ms"),
        F.max("duration_ms").alias("max_duration_ms"),
        F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
        F.sum(F.col("is_error").cast("int")).cast("long").alias("error_span_count"),
        F.sum(((F.col("http_status") >= 400) & (F.col("http_status") < 500)).cast("int")).cast("long").alias("http_4xx_count"),
        F.sum((F.col("http_status") >= 500).cast("int")).cast("long").alias("http_5xx_count"),
        F.countDistinct("operation_name").cast("long").alias("unique_operation_count"),
    )


def build_graph_features(spark: SparkSession, silver_root: Path, window_seconds: int) -> DataFrame:
    edges = with_window(read_silver(spark, silver_root, "trace_edges"), "timestamp", window_seconds)

    out_features = (
        edges.where(F.col("source_service").isNotNull())
        .withColumnRenamed("source_service", "service_name")
        .groupBy(KEY_COLS)
        .agg(
            F.countDistinct("target_service").cast("long").alias("out_degree"),
            F.count("*").cast("long").alias("weighted_call_count"),
            F.avg("duration_ms").alias("avg_edge_latency_ms"),
            F.max("duration_ms").alias("max_edge_latency_ms"),
            F.sum(F.col("is_error").cast("int")).cast("long").alias("error_edge_count"),
            F.countDistinct("target_service").cast("long").alias("unique_peer_service_count"),
        )
    )

    in_features = (
        edges.where(F.col("target_service").isNotNull())
        .withColumnRenamed("target_service", "service_name")
        .groupBy(KEY_COLS)
        .agg(F.countDistinct("source_service").cast("long").alias("in_degree"))
    )

    return out_features.join(in_features, KEY_COLS, "full_outer")


def join_feature_frames(frames: List[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.join(right, KEY_COLS, "full_outer"), frames)


def add_labels(
    spark: SparkSession,
    features: DataFrame,
    silver_root: Path,
    relaxed_seconds: int,
) -> DataFrame:
    anomalies_path = silver_root / "anomalies"
    if not table_exists(anomalies_path):
        return features.withColumn("label", F.lit(0))

    raw_anomalies = spark.read.parquet(str(anomalies_path)).where(F.col("anomaly_timestamp").isNotNull())
    anomaly_cols = raw_anomalies.columns
    anomaly_select = [
        F.col("case_id").alias("anomaly_case_id"),
        F.col("service_name").alias("anomaly_service_name"),
        F.col("anomaly_timestamp"),
    ]
    if "trace_id" in anomaly_cols:
        anomaly_select.append(F.col("trace_id"))
    else:
        anomaly_select.append(F.lit(None).cast("string").alias("trace_id"))

    direct_anomalies = raw_anomalies.select(*anomaly_select).where(F.col("anomaly_service_name").isNotNull())
    anomalies = direct_anomalies

    spans_path = silver_root / "spans"
    if table_exists(spans_path):
        trace_services = (
            spark.read.parquet(str(spans_path))
            .select(
                F.col("case_id").alias("span_case_id"),
                F.col("trace_id").alias("span_trace_id"),
                F.col("service_name").alias("span_service_name"),
            )
            .where(F.col("span_trace_id").isNotNull() & F.col("span_service_name").isNotNull())
            .distinct()
        )
        trace_anomalies = (
            direct_anomalies.where(F.col("trace_id").isNotNull())
            .join(
                trace_services,
                (F.col("anomaly_case_id") == F.col("span_case_id"))
                & (F.col("trace_id") == F.col("span_trace_id")),
                "inner",
            )
            .select(
                "anomaly_case_id",
                F.col("span_service_name").alias("anomaly_service_name"),
                "anomaly_timestamp",
                "trace_id",
            )
        )
        anomalies = direct_anomalies.unionByName(trace_anomalies).distinct()

    anomalies = (
        anomalies.select("anomaly_case_id", "anomaly_service_name", "anomaly_timestamp")
        .where(F.col("anomaly_service_name").isNotNull())
        .distinct()
    )

    feature_cols = features.columns
    feature_ids = features.withColumn("_feature_id", F.monotonically_increasing_id())
    labeled_ids = (
        feature_ids.alias("f")
        .join(
            anomalies.alias("a"),
            (F.col("f.case_id") == F.col("a.anomaly_case_id"))
            & (F.col("f.service_name") == F.col("a.anomaly_service_name"))
            & (
                F.col("a.anomaly_timestamp").between(
                    F.col("f.window_start") - F.expr(f"INTERVAL {relaxed_seconds} SECONDS"),
                    F.col("f.window_end") + F.expr(f"INTERVAL {relaxed_seconds} SECONDS"),
                )
            ),
            "left",
        )
        .groupBy("_feature_id")
        .agg(F.max(F.col("a.anomaly_timestamp").isNotNull().cast("int")).alias("label"))
    )

    return (
        feature_ids.join(labeled_ids, "_feature_id", "left")
        .drop("_feature_id")
        .select(*(feature_cols + [F.coalesce(F.col("label"), F.lit(0)).cast("int").alias("label")]))
    )


def fill_feature_nulls(features: DataFrame) -> DataFrame:
    fill_values = {}
    for name, dtype in features.dtypes:
        if name in KEY_COLS:
            continue
        if dtype in {"bigint", "int", "double", "float", "long"}:
            fill_values[name] = 0
    return features.fillna(fill_values)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build gold window-level feature table from silver telemetry.")
    parser.add_argument("--config", default="configs/project_config.json")
    parser.add_argument("--silver-root", default=None)
    parser.add_argument("--gold-root", default=None)
    parser.add_argument("--window-seconds", type=int, default=None)
    parser.add_argument("--relaxed-label-seconds", type=int, default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    silver_root = Path(args.silver_root or config["silver_root"])
    gold_root = Path(args.gold_root or config["gold_root"])
    window_seconds = args.window_seconds or int(config.get("default_window_seconds", 60))
    dataset_timezone = config.get("dataset_timezone", "Asia/Shanghai")
    relaxed_label_seconds = args.relaxed_label_seconds
    if relaxed_label_seconds is None:
        relaxed_label_seconds = int(config.get("relaxed_label_seconds", 120))

    print(f"silver_root={silver_root}")
    print(f"gold_root={gold_root}")
    print(f"window_seconds={window_seconds}")
    print(f"relaxed_label_seconds={relaxed_label_seconds}")
    print(f"dataset_timezone={dataset_timezone}")

    spark = build_spark(timezone=dataset_timezone)
    spark.sparkContext.setLogLevel("WARN")

    frames = [
        build_log_features(spark, silver_root, window_seconds),
        build_metric_features(spark, silver_root, window_seconds),
        build_trace_features(spark, silver_root, window_seconds),
        build_graph_features(spark, silver_root, window_seconds),
    ]

    features = join_feature_frames(frames)
    features = add_labels(spark, features, silver_root, relaxed_label_seconds)
    features = fill_feature_nulls(features).persist()

    output_path = gold_root / "window_features"
    features.write.mode("overwrite").partitionBy("case_id").parquet(str(output_path))
    print(f"Wrote gold window features to {output_path}")
    print(f"gold_window_rows={features.count()}")
    features.groupBy("label").count().orderBy("label").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
