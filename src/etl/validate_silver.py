import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("train-ticket-validate-silver")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def validate_table(spark: SparkSession, silver_root: Path, table: str) -> None:
    path = silver_root / table
    if not path.exists():
        print(f"{table}: MISSING at {path}")
        return

    df = spark.read.parquet(str(path))
    row_count = df.count()
    case_count = df.select("case_id").distinct().count() if "case_id" in df.columns else 0
    print(f"{table}: rows={row_count}, cases={case_count}, columns={len(df.columns)}")

    if "case_id" in df.columns:
        df.groupBy("case_id").count().orderBy("case_id").show(20, truncate=False)

    timestamp_cols = [col for col in ["timestamp", "anomaly_timestamp"] if col in df.columns]
    for col in timestamp_cols:
        df.select(F.min(col).alias(f"min_{col}"), F.max(col).alias(f"max_{col}")).show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate silver parquet tables.")
    parser.add_argument("--silver-root", default="data_lake/silver")
    parser.add_argument("--tables", default="logs,metrics,spans,trace_edges,anomalies")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver_root = Path(args.silver_root)
    for table in [item.strip() for item in args.tables.split(",") if item.strip()]:
        validate_table(spark, silver_root, table)

    spark.stop()


if __name__ == "__main__":
    main()
