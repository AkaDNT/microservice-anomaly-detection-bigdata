import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


THRESHOLD_PATTERN = re.compile(r"^(?P<model>.+?) (?P<algorithm>random_forest|logistic_regression) threshold: (?P<payload>\{.+\})$")
RATIO_PATTERN = re.compile(r"negative_positive_ratio=(?P<ratio>\d+)")


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def as_float(value: Any) -> float:
    return float(value or 0.0)


def as_int(value: Any) -> int:
    return int(value or 0)


def algorithm_short_name(value: str) -> str:
    lowered = value.lower()
    if "randomforest" in lowered or "random_forest" in lowered:
        return "random_forest"
    if "logistic" in lowered:
        return "logistic_regression"
    return value


def rows_from_summary(path: Path, family: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []

    payload = load_json(path)
    rows: List[Dict[str, Any]] = []
    for item in payload:
        tuning = item.get("threshold_tuning", {})
        split = item.get("split", {})
        rows.append(
            {
                "source": path.name,
                "family": family,
                "algorithm": algorithm_short_name(item.get("algorithm", "")),
                "model": item.get("baseline") or item.get("model") or "",
                "negative_positive_ratio": split.get("negative_positive_ratio", ""),
                "threshold": as_float(tuning.get("best_threshold")),
                "precision": as_float(tuning.get("best_precision")),
                "recall": as_float(tuning.get("best_recall")),
                "f1": as_float(tuning.get("best_f1")),
                "tp": as_int(tuning.get("best_tp")),
                "fp": as_int(tuning.get("best_fp")),
                "fn": as_int(tuning.get("best_fn")),
                "tn": as_int(tuning.get("best_tn")),
            }
        )
    return rows


def rows_from_fusion_logs(log_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in sorted(log_dir.glob("train_fusion_*.log")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        ratio_match = RATIO_PATTERN.search(text)
        ratio = ratio_match.group("ratio") if ratio_match else ""
        for line in text.splitlines():
            match = THRESHOLD_PATTERN.match(line.strip())
            if not match:
                continue
            payload = json.loads(match.group("payload"))
            rows.append(
                {
                    "source": path.name,
                    "family": "fusion_log_history",
                    "algorithm": match.group("algorithm"),
                    "model": match.group("model"),
                    "negative_positive_ratio": ratio,
                    "threshold": as_float(payload.get("best_threshold")),
                    "precision": as_float(payload.get("best_precision")),
                    "recall": as_float(payload.get("best_recall")),
                    "f1": as_float(payload.get("best_f1")),
                    "tp": as_int(payload.get("best_tp")),
                    "fp": as_int(payload.get("best_fp")),
                    "fn": as_int(payload.get("best_fn")),
                    "tn": as_int(payload.get("best_tn")),
                }
            )
    return rows


def fallback_path(path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return path.with_name(f"{path.stem}_{timestamp}{path.suffix}")


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(rows)
    fieldnames = [
        "source",
        "family",
        "algorithm",
        "model",
        "negative_positive_ratio",
        "threshold",
        "precision",
        "recall",
        "f1",
        "tp",
        "fp",
        "fn",
        "tn",
    ]
    target = path
    try:
        file = target.open("w", encoding="utf-8", newline="")
    except PermissionError:
        target = fallback_path(path)
        file = target.open("w", encoding="utf-8", newline="")

    with file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(materialized)
    return target


def markdown_table(rows: List[Dict[str, Any]], limit: int = 10) -> str:
    lines = [
        "| Rank | Family | Algorithm | Model | Ratio | F1 | Precision | Recall | TP | FP | FN |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for index, row in enumerate(rows[:limit], start=1):
        lines.append(
            "| {rank} | {family} | {algorithm} | {model} | {ratio} | {f1:.4f} | {precision:.4f} | {recall:.4f} | {tp} | {fp} | {fn} |".format(
                rank=index,
                family=row["family"],
                algorithm=row["algorithm"],
                model=row["model"],
                ratio=row["negative_positive_ratio"] or "-",
                f1=row["f1"],
                precision=row["precision"],
                recall=row["recall"],
                tp=row["tp"],
                fp=row["fp"],
                fn=row["fn"],
            )
        )
    return "\n".join(lines)


def write_markdown(path: Path, rows: List[Dict[str, Any]]) -> Path:
    best = rows[0] if rows else None
    content = [
        "# Sprint 6 Dashboard Summary",
        "",
        "This file is generated from `reports/metrics/*.json` and `reports/models/train_fusion_*.log`.",
        "",
        "## Best Model",
        "",
    ]
    if best:
        content.extend(
            [
                f"- Family: `{best['family']}`",
                f"- Algorithm: `{best['algorithm']}`",
                f"- Model: `{best['model']}`",
                f"- Negative/positive ratio: `{best['negative_positive_ratio'] or '-'}`",
                f"- Threshold: `{best['threshold']:.2f}`",
                f"- Precision: `{best['precision']:.4f}`",
                f"- Recall: `{best['recall']:.4f}`",
                f"- F1-score: `{best['f1']:.4f}`",
                f"- Confusion matrix: TP `{best['tp']}`, FP `{best['fp']}`, FN `{best['fn']}`, TN `{best['tn']}`",
            ]
        )
    else:
        content.append("- No model metrics found.")

    content.extend(
        [
            "",
            "## Top Models",
            "",
            markdown_table(rows, limit=12),
            "",
            "## Suggested Visuals",
            "",
            "- Bar chart: `f1` by `family`, `algorithm`, and `model`.",
            "- Scatter plot: `precision` vs `recall`, colored by `family`.",
            "- Confusion matrix cards: TP, FP, FN, TN for the selected best model.",
            "- Table filter: compare baseline, fusion summary, and fusion log history.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    target = path
    try:
        target.write_text("\n".join(content), encoding="utf-8")
    except PermissionError:
        target = fallback_path(path)
        target.write_text("\n".join(content), encoding="utf-8")
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Sprint 6 dashboard-ready CSV and Markdown assets.")
    parser.add_argument("--metrics-dir", default="reports/metrics")
    parser.add_argument("--models-dir", default="reports/models")
    parser.add_argument("--output-dir", default="reports/dashboard")
    args = parser.parse_args()

    metrics_dir = Path(args.metrics_dir)
    models_dir = Path(args.models_dir)
    output_dir = Path(args.output_dir)

    rows: List[Dict[str, Any]] = []
    rows.extend(rows_from_summary(metrics_dir / "baseline_summary.json", "baseline_summary"))
    rows.extend(rows_from_summary(metrics_dir / "fusion_summary.json", "fusion_summary_latest"))
    rows.extend(rows_from_fusion_logs(models_dir))
    rows.sort(key=lambda item: (item["f1"], item["precision"], item["recall"]), reverse=True)

    csv_path = write_csv(output_dir / "model_comparison.csv", rows)
    markdown_path = write_markdown(output_dir / "dashboard_summary.md", rows)
    print(f"wrote {csv_path}")
    print(f"wrote {markdown_path}")


if __name__ == "__main__":
    main()
