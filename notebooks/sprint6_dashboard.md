# Sprint 6 Dashboard Notebook

This lightweight notebook is the dashboard substitute for local execution. It uses generated CSV/Markdown assets instead of requiring Superset or Zeppelin setup on every machine.

## Inputs

- Gold feature table: `data_lake/gold/window_features`
- Baseline metrics: `reports/metrics/baseline_summary.json`
- Fusion metrics: `reports/metrics/fusion_summary.json`
- Fusion run history: `reports/models/train_fusion_*.log`

## Build Dashboard Assets

```bash
python src/reports/build_dashboard_assets.py
```

Outputs:

- `reports/dashboard/model_comparison.csv`
- `reports/dashboard/dashboard_summary.md`

## Visuals To Show In Report

1. Model comparison table sorted by F1-score.
2. Bar chart of F1-score by model family.
3. Precision vs recall scatter plot.
4. Confusion matrix cards for the best model.
5. Pipeline run log from `reports/pipeline/pipeline.log`.

## Current Key Result

The best Sprint 5 result found so far is:

- Model: Logistic Regression `selected_logs_metrics_graph`
- Negative/positive ratio: `50:1`
- Threshold: `0.99`
- Precision: `0.0779`
- Recall: `0.2000`
- F1-score: `0.1121`
- Confusion matrix: TP `6`, FP `71`, FN `24`, TN `133035`

This improves over the best Sprint 4 single-source baseline:

- Logistic Regression `metrics-only`
- F1-score: `0.0890`

## Optional Superset Setup

If Superset is available, import `reports/dashboard/model_comparison.csv` as a dataset and create:

- Table chart: model comparison.
- Bar chart: `f1` grouped by `family`, `algorithm`, `model`.
- Scatter chart: x=`precision`, y=`recall`, color=`family`.
- Big number cards: `tp`, `fp`, `fn`, `tn` filtered to the best model.
