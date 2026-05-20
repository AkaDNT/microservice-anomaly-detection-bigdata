import argparse

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


LOG_EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_index", T.LongType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("service_name", T.StringType(), True),
        T.StructField("level", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("event_template", T.StringType(), True),
        T.StructField("content", T.StringType(), True),
    ]
)

LOG_FEATURE_COLUMNS = [
    "log_count",
    "error_count",
    "warn_count",
    "info_count",
    "unique_event_id_count",
    "span_reported_count",
    "top_event_frequency",
    "template_entropy",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("train-ticket-logs-only-streaming-alerts")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def parse_events(kafka_df):
    parsed = kafka_df.select(F.from_json(F.col("value").cast("string"), LOG_EVENT_SCHEMA).alias("event"))
    return (
        parsed.select("event.*")
        .withColumn("event_timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("level", F.upper(F.coalesce(F.col("level"), F.lit(""))))
        .withColumn("event_id", F.coalesce(F.col("event_id"), F.lit("")))
        .withColumn("content", F.coalesce(F.col("content"), F.lit("")))
        .where(F.col("event_timestamp").isNotNull())
        .where(F.col("service_name").isNotNull())
    )


def build_log_features(events, window_seconds: int, watermark: str):
    base = events.withWatermark("event_timestamp", watermark)

    features = base.groupBy("service_name", F.window("event_timestamp", f"{window_seconds} seconds")).agg(
        F.count("*").cast("double").alias("log_count"),
        F.sum((F.col("level") == "ERROR").cast("int")).cast("double").alias("error_count"),
        F.sum((F.col("level") == "WARN").cast("int")).cast("double").alias("warn_count"),
        F.sum((F.col("level") == "INFO").cast("int")).cast("double").alias("info_count"),
        F.approx_count_distinct("event_id").cast("double").alias("unique_event_id_count"),
        F.sum(F.col("content").contains("Span reported").cast("int")).cast("double").alias("span_reported_count"),
    )

    return (
        features
        # Streaming update mode cannot join two streaming aggregates. For the
        # realtime demo, use log_count as a conservative upper-bound proxy.
        .withColumn("top_event_frequency", F.col("log_count"))
        .withColumn("template_entropy", F.lit(0.0))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


def build_alerts(features, model_path: str, threshold: float, emit_all: bool):
    model = PipelineModel.load(model_path)
    scored = model.transform(features.fillna(0))
    positive_probability = vector_to_array("probability")[1]
    with_probability = scored.withColumn("positive_probability", positive_probability).withColumn(
        "alert",
        F.when(F.col("positive_probability") >= F.lit(threshold), F.lit("anomaly")).otherwise(F.lit("normal")),
    )
    if not emit_all:
        with_probability = with_probability.where(F.col("positive_probability") >= F.lit(threshold))

    return with_probability.select(
        F.to_json(
            F.struct(
                F.col("service_name"),
                F.col("window_start").cast("string").alias("window_start"),
                F.col("window_end").cast("string").alias("window_end"),
                F.lit("logs_only_logistic_regression").alias("model"),
                F.col("positive_probability").alias("probability"),
                F.lit(threshold).alias("threshold"),
                F.col("alert"),
                *[F.col(column) for column in LOG_FEATURE_COLUMNS],
            )
        ).alias("value")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Logs-only realtime anomaly alerts from Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="train-ticket-logs")
    parser.add_argument("--alert-topic", default="train-ticket-alerts")
    parser.add_argument("--checkpoint-dir", default="data_lake/checkpoints/logs_only_alerts")
    parser.add_argument("--model-path", default="reports/models/artifacts/baseline_logs_only_logistic_regression")
    parser.add_argument("--threshold", type=float, default=0.96)
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--watermark", default="5 minutes")
    parser.add_argument("--output-mode", choices=["kafka", "console"], default="kafka")
    parser.add_argument("--starting-offsets", choices=["latest", "earliest"], default="latest")
    parser.add_argument(
        "--emit-all",
        action="store_true",
        help="Emit all scored windows instead of only anomaly windows. Useful for debugging/demo.",
    )
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    events = parse_events(kafka_df)
    features = build_log_features(events, args.window_seconds, args.watermark)
    alerts = build_alerts(features, args.model_path, args.threshold, args.emit_all)

    if args.output_mode == "console":
        query = (
            alerts.writeStream.outputMode("update")
            .option("checkpointLocation", args.checkpoint_dir)
            .format("console")
            .option("truncate", "false")
            .start()
        )
    else:
        query = (
            alerts.writeStream.outputMode("append")
            .option("checkpointLocation", args.checkpoint_dir)
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", args.alert_topic)
            .start()
        )

    query.awaitTermination()


if __name__ == "__main__":
    main()
