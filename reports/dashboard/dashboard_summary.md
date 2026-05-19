# Sprint 6 Dashboard Summary

This file is generated from `reports/metrics/*.json` and `reports/models/train_fusion_*.log`.

## Best Model

- Family: `fusion_summary_latest`
- Algorithm: `logistic_regression`
- Model: `selected_logs_metrics_graph`
- Negative/positive ratio: `50`
- Threshold: `0.99`
- Precision: `0.0779`
- Recall: `0.2000`
- F1-score: `0.1121`
- Confusion matrix: TP `6`, FP `71`, FN `24`, TN `133035`

## Top Models

| Rank | Family | Algorithm | Model | Ratio | F1 | Precision | Recall | TP | FP | FN |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | fusion_summary_latest | logistic_regression | selected_logs_metrics_graph | 50 | 0.1121 | 0.0779 | 0.2000 | 6 | 71 | 24 |
| 2 | fusion_log_history | logistic_regression | selected_logs_metrics_graph | 50 | 0.1121 | 0.0779 | 0.2000 | 6 | 71 | 24 |
| 3 | fusion_log_history | logistic_regression | selected_logs_metrics_graph | 50 | 0.1121 | 0.0779 | 0.2000 | 6 | 71 | 24 |
| 4 | baseline_summary | logistic_regression | metrics_only | - | 0.0890 | 0.0472 | 0.7667 | 23 | 464 | 7 |
| 5 | baseline_summary | logistic_regression | logs_only | - | 0.0889 | 0.0533 | 0.2667 | 8 | 142 | 22 |
| 6 | fusion_log_history | logistic_regression | selected_logs_metrics_trace_latency | 50 | 0.0710 | 0.0432 | 0.2000 | 6 | 133 | 24 |
| 7 | fusion_log_history | logistic_regression | logs_metrics | 50 | 0.0654 | 0.0345 | 0.6333 | 19 | 532 | 11 |
| 8 | fusion_log_history | logistic_regression | selected_logs_metrics | 50 | 0.0652 | 0.0344 | 0.6333 | 19 | 534 | 11 |
| 9 | fusion_log_history | logistic_regression | logs_metrics_traces | - | 0.0558 | 0.0324 | 0.2000 | 6 | 179 | 24 |
| 10 | fusion_log_history | logistic_regression | logs_metrics_traces_graph | - | 0.0533 | 0.0308 | 0.2000 | 6 | 189 | 24 |
| 11 | fusion_log_history | logistic_regression | selected_logs_metrics_graph | 20 | 0.0530 | 0.0277 | 0.6000 | 18 | 631 | 12 |
| 12 | fusion_log_history | logistic_regression | selected_logs_metrics | 20 | 0.0499 | 0.0260 | 0.6333 | 19 | 712 | 11 |

## Suggested Visuals

- Bar chart: `f1` by `family`, `algorithm`, and `model`.
- Scatter plot: `precision` vs `recall`, colored by `family`.
- Confusion matrix cards: TP, FP, FN, TN for the selected best model.
- Table filter: compare baseline, fusion summary, and fusion log history.
