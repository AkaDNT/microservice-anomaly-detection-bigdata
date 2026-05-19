# Dashboard Assets

This folder contains Sprint 6 dashboard-ready artifacts.

Generated files:

- `model_comparison.csv`: model metrics table sorted by F1-score.
- `dashboard_summary.md`: compact dashboard summary for report/demo.

Regenerate from current metrics and fusion logs:

```bash
python src/reports/build_dashboard_assets.py
```

Inputs:

- `reports/metrics/baseline_summary.json`
- `reports/metrics/fusion_summary.json`
- `reports/models/train_fusion_*.log`

Suggested visuals:

- F1-score bar chart by model.
- Precision/recall scatter plot.
- Confusion matrix cards for the best model.
- Comparison table for baseline vs fusion history.
