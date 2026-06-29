import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DEFAULT_TRAIN_CASE_PREFIXES = [f"case_{idx:02d}_" for idx in range(1, 8)]

BASELINES: Dict[str, List[str]] = {
    "logs": [
        "log_count",
        "error_count",
        "warn_count",
        "info_count",
        "unique_event_id_count",
        "span_reported_count",
        "top_event_frequency",
        "template_entropy",
    ],
    "metrics": [
        "cpu_mean",
        "cpu_max",
        "cpu_std",
        "memory_mean",
        "memory_max",
        "memory_std",
        "network_mean",
        "network_max",
        "node_memory_available_mean",
        "node_memory_total_mean",
        "cpu_rate_mean",
    ],
    "traces": [
        "trace_count",
        "span_count",
        "avg_duration_ms",
        "max_duration_ms",
        "p95_duration_ms",
        "error_span_count",
        "http_4xx_count",
        "http_5xx_count",
        "unique_operation_count",
    ],
}

THRESHOLDS = [round(value / 100.0, 2) for value in range(1, 100)] + [0.995, 0.999]


def build_spark(app_name: str = "train-ticket-train-baselines") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def parse_csv_arg(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_train_cases(all_cases: List[str], explicit_cases: str | None) -> List[str]:
    if explicit_cases:
        return parse_csv_arg(explicit_cases)
    return [case for case in all_cases if any(case.startswith(prefix) for prefix in DEFAULT_TRAIN_CASE_PREFIXES)]


def add_class_weights(df: DataFrame, mode: str) -> Tuple[DataFrame, Dict[str, int | float | str]]:
    counts = {row["label"]: row["count"] for row in df.groupBy("label").count().collect()}
    negative_count = int(counts.get(0, 0))
    positive_count = int(counts.get(1, 0))
    summary: Dict[str, int | float | str] = {
        "mode": mode,
        "negative_count": negative_count,
        "positive_count": positive_count,
    }
    if mode == "none":
        return df.withColumn("class_weight", F.lit(1.0)), summary
    if mode != "balanced":
        raise ValueError(f"Unsupported class weight mode: {mode}")
    if positive_count == 0 or negative_count == 0:
        return df.withColumn("class_weight", F.lit(1.0)), summary

    total_count = negative_count + positive_count
    negative_weight = total_count / (2.0 * negative_count)
    positive_weight = total_count / (2.0 * positive_count)
    summary["negative_weight"] = negative_weight
    summary["positive_weight"] = positive_weight
    weighted = df.withColumn(
        "class_weight",
        F.when(F.col("label") == 1, F.lit(positive_weight)).otherwise(F.lit(negative_weight)),
    )
    return weighted, summary


def downsample_negatives(df: DataFrame, negative_positive_ratio: int, seed: int = 42) -> DataFrame:
    if negative_positive_ratio <= 0:
        return df

    counts = {row["label"]: row["count"] for row in df.groupBy("label").count().collect()}
    negative_count = int(counts.get(0, 0))
    positive_count = int(counts.get(1, 0))
    if positive_count == 0 or negative_count == 0:
        return df

    target_negative_count = positive_count * negative_positive_ratio
    if target_negative_count >= negative_count:
        return df

    positives = df.where(F.col("label") == 1)
    negatives = df.where(F.col("label") == 0).sample(
        withReplacement=False,
        fraction=target_negative_count / negative_count,
        seed=seed,
    )
    return positives.unionByName(negatives)


def confusion_metrics(predictions: DataFrame, prediction_col: str = "prediction") -> Dict[str, float | int]:
    row = predictions.agg(
        F.sum(((F.col("label") == 1) & (F.col(prediction_col) == 1)).cast("int")).alias("tp"),
        F.sum(((F.col("label") == 0) & (F.col(prediction_col) == 1)).cast("int")).alias("fp"),
        F.sum(((F.col("label") == 1) & (F.col(prediction_col) == 0)).cast("int")).alias("fn"),
        F.sum(((F.col("label") == 0) & (F.col(prediction_col) == 0)).cast("int")).alias("tn"),
    ).first()

    tp = int(row["tp"] or 0)
    fp = int(row["fp"] or 0)
    fn = int(row["fn"] or 0)
    tn = int(row["tn"] or 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (tp + tn) / (tp + fp + fn + tn) if tp + fp + fn + tn else 0.0

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
    }


def tune_threshold(predictions: DataFrame) -> Dict[str, float | int]:
    scored = predictions.withColumn("positive_probability", vector_to_array("probability")[1]).cache()
    best_metrics: Dict[str, float | int] | None = None
    best_threshold = 0.5

    for threshold in THRESHOLDS:
        thresholded = scored.withColumn(
            "threshold_prediction",
            (F.col("positive_probability") >= F.lit(threshold)).cast("double"),
        )
        metrics = confusion_metrics(thresholded, "threshold_prediction")
        if best_metrics is None or (
            metrics["f1"],
            metrics["recall"],
            metrics["precision"],
        ) > (
            best_metrics["f1"],
            best_metrics["recall"],
            best_metrics["precision"],
        ):
            best_metrics = metrics
            best_threshold = threshold

    if best_metrics is None:
        return {"best_threshold": 0.5}

    return {
        "best_threshold": best_threshold,
        "best_precision": best_metrics["precision"],
        "best_recall": best_metrics["recall"],
        "best_f1": best_metrics["f1"],
        "best_tp": best_metrics["tp"],
        "best_fp": best_metrics["fp"],
        "best_fn": best_metrics["fn"],
        "best_tn": best_metrics["tn"],
    }


def feature_importance(feature_cols: List[str], model_stage: object) -> List[Dict[str, float]]:
    importances = getattr(model_stage, "featureImportances", None)
    if importances is None:
        return []

    pairs = [
        {"feature": feature, "importance": float(importance)}
        for feature, importance in zip(feature_cols, importances.toArray().tolist())
    ]
    return sorted(pairs, key=lambda item: item["importance"], reverse=True)


def train_one_baseline(
    name: str,
    feature_cols: List[str],
    train_df: DataFrame,
    test_df: DataFrame,
    algorithm: str,
) -> Tuple[Dict[str, object], object]:
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=False, withStd=True)
    if algorithm == "logistic_regression":
        classifier = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            weightCol="class_weight",
            maxIter=50,
            regParam=0.05,
            elasticNetParam=0.0,
        )
        algorithm_name = "Spark ML LogisticRegression"
    elif algorithm == "random_forest":
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            weightCol="class_weight",
            numTrees=80,
            maxDepth=8,
            seed=42,
        )
        algorithm_name = "Spark ML RandomForestClassifier"
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")

    pipeline = Pipeline(stages=[assembler, scaler, classifier])
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df).cache()

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )

    metrics = confusion_metrics(predictions)
    metrics["area_under_roc"] = float(evaluator.evaluate(predictions))
    metrics["area_under_pr"] = float(pr_evaluator.evaluate(predictions))
    threshold_metrics = tune_threshold(predictions)

    return {
        "baseline": f"{name}_only",
        "algorithm": algorithm_name,
        "feature_columns": feature_cols,
        "metrics": metrics,
        "threshold_tuning": threshold_metrics,
        "feature_importance": feature_importance(feature_cols, model.stages[-1]),
    }, model


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, sort_keys=True)
        file.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Sprint 4 single-source baseline models from gold features.")
    parser.add_argument("--config", default="configs/project_config.json")
    parser.add_argument("--gold-root", default=None)
    parser.add_argument("--table", default="window_features")
    parser.add_argument("--output-dir", default="reports/metrics")
    parser.add_argument("--model-output-dir", default="reports/models/artifacts")
    parser.add_argument(
        "--skip-model-artifacts",
        action="store_true",
        help="Do not save Spark ML PipelineModel artifacts.",
    )
    parser.add_argument("--train-cases", default=None, help="Comma-separated case_id list. Default: case_01 to case_07.")
    parser.add_argument(
        "--include-random-forest",
        action="store_true",
        help="Also train Random Forest single-source baselines. Slower, but useful for Sprint 4 comparison.",
    )
    parser.add_argument(
        "--negative-positive-ratio",
        type=int,
        default=50,
        help="Downsample train negatives to this ratio per positive. Use 0 to disable.",
    )
    parser.add_argument(
        "--class-weight-mode",
        choices=["none", "balanced"],
        default="none",
        help="Use no class weights by default after downsampling; choose balanced for higher recall and more false positives.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    gold_root = Path(args.gold_root or config["gold_root"])
    input_path = gold_root / args.table
    output_dir = Path(args.output_dir)
    model_output_dir = Path(args.model_output_dir)

    print(f"gold_table={input_path}")
    print(f"output_dir={output_dir}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(str(input_path)).fillna(0)
    all_cases = [row["case_id"] for row in df.select("case_id").distinct().orderBy("case_id").collect()]
    train_cases = resolve_train_cases(all_cases, args.train_cases)
    test_cases = [case for case in all_cases if case not in set(train_cases)]

    if not train_cases or not test_cases:
        raise ValueError("Train/test split must contain at least one case on each side.")

    train_df = df.where(F.col("case_id").isin(train_cases)).cache()
    test_df = df.where(F.col("case_id").isin(test_cases)).cache()
    original_train_rows = train_df.count()
    original_train_counts = {row["label"]: row["count"] for row in train_df.groupBy("label").count().collect()}
    train_df = downsample_negatives(train_df, args.negative_positive_ratio).cache()
    train_df, train_counts = add_class_weights(train_df, args.class_weight_mode)
    test_df = test_df.withColumn("class_weight", F.lit(1.0))
    train_rows = train_df.count()
    test_rows = test_df.count()
    test_label_counts = {row["label"]: row["count"] for row in test_df.groupBy("label").count().collect()}

    split_summary = {
        "split_strategy": "case_id split",
        "train_cases": train_cases,
        "test_cases": test_cases,
        "original_train_rows": original_train_rows,
        "original_train_label_counts": original_train_counts,
        "negative_positive_ratio": args.negative_positive_ratio,
        "class_weight_mode": args.class_weight_mode,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "train_label_counts": train_counts,
        "test_label_counts": test_label_counts,
        "test_positive_rate": float(test_label_counts.get(1, 0) / test_rows) if test_rows else 0.0,
    }
    print(json.dumps(split_summary, indent=2, sort_keys=True))

    algorithms = ["logistic_regression"]
    if args.include_random_forest:
        algorithms.append("random_forest")

    results = []
    for algorithm in algorithms:
        for name, feature_cols in BASELINES.items():
            missing = [col for col in feature_cols if col not in df.columns]
            if missing:
                raise ValueError(f"Missing columns for {name} baseline: {missing}")

            result, model = train_one_baseline(name, feature_cols, train_df, test_df, algorithm)
            result["split"] = split_summary
            if not args.skip_model_artifacts:
                artifact_path = model_output_dir / f"baseline_{name}_only_{algorithm}"
                model.write().overwrite().save(str(artifact_path))
                result["model_artifact_path"] = str(artifact_path)
            results.append(result)
            suffix = "" if algorithm == "logistic_regression" else f"_{algorithm}"
            write_json(output_dir / f"baseline_{name}{suffix}.json", result)
            print(f"{result['baseline']} {algorithm}: {json.dumps(result['metrics'], sort_keys=True)}")
            print(f"{result['baseline']} {algorithm} threshold: {json.dumps(result['threshold_tuning'], sort_keys=True)}")

    write_json(output_dir / "baseline_summary.json", results)
    spark.stop()


if __name__ == "__main__":
    main()
