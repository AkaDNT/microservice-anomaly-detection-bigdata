import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


KEY_COLUMNS = {"case_id", "service_name", "window_start", "window_end"}


def build_spark(timezone: str = "Asia/Shanghai") -> SparkSession:
    return (
        SparkSession.builder.appName("train-ticket-validate-gold")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", timezone)
        .getOrCreate()
    )


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate gold window feature table.")
    parser.add_argument("--config", default="configs/project_config.json")
    parser.add_argument("--gold-root", default="data_lake/gold")
    parser.add_argument("--table", default="window_features")
    args = parser.parse_args()

    path = Path(args.gold_root) / args.table
    if not path.exists():
        raise FileNotFoundError(f"Missing gold table: {path}")

    config = load_config(args.config)
    spark = build_spark(timezone=config.get("dataset_timezone", "Asia/Shanghai"))
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(str(path))
    print(f"{args.table}: rows={df.count()}, cases={df.select('case_id').distinct().count()}, columns={len(df.columns)}")
    df.groupBy("case_id").count().orderBy("case_id").show(20, truncate=False)
    df.groupBy("label").count().orderBy("label").show(truncate=False)
    df.select(
        F.min("window_start").alias("min_window_start"),
        F.max("window_end").alias("max_window_end"),
    ).show(truncate=False)
    df.select("case_id", "service_name", "window_start", "window_end", "label").show(20, truncate=False)

    numeric_feature_columns = [
        name
        for name, dtype in df.dtypes
        if name not in KEY_COLUMNS | {"label"} and dtype in {"bigint", "int", "double", "float", "long"}
    ]
    if numeric_feature_columns:
        checks = []
        for column in numeric_feature_columns:
            checks.append(F.sum((F.col(column) != 0).cast("long")).alias(f"{column}__nonzero"))
            checks.append(F.min(column).alias(f"{column}__min"))
            checks.append(F.max(column).alias(f"{column}__max"))

        stats = df.agg(*checks).first().asDict()
        all_zero_columns = [
            column
            for column in numeric_feature_columns
            if int(stats.get(f"{column}__nonzero") or 0) == 0
        ]
        if all_zero_columns:
            print("All-zero numeric feature columns:")
            for column in all_zero_columns:
                print(
                    f"- {column}: "
                    f"min={stats.get(f'{column}__min')}, "
                    f"max={stats.get(f'{column}__max')}"
                )
            raise ValueError(f"Gold table has all-zero numeric feature columns: {all_zero_columns}")

    spark.stop()


if __name__ == "__main__":
    main()
