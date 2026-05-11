import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("train-ticket-validate-gold")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate gold window feature table.")
    parser.add_argument("--gold-root", default="data_lake/gold")
    parser.add_argument("--table", default="window_features")
    args = parser.parse_args()

    path = Path(args.gold_root) / args.table
    if not path.exists():
        raise FileNotFoundError(f"Missing gold table: {path}")

    spark = build_spark()
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

    spark.stop()


if __name__ == "__main__":
    main()
