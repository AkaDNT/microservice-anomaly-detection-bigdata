import json
import tempfile
import unittest
from pathlib import Path

from src.reports.build_dashboard_assets import rows_from_fusion_logs, rows_from_summary


class DashboardAssetsTest(unittest.TestCase):
    def test_rows_from_summary_extracts_threshold_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "baseline_summary.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "algorithm": "Spark ML LogisticRegression",
                            "baseline": "metrics_only",
                            "split": {"negative_positive_ratio": 50},
                            "threshold_tuning": {
                                "best_threshold": 0.9,
                                "best_precision": 0.1,
                                "best_recall": 0.2,
                                "best_f1": 0.1333,
                                "best_tp": 2,
                                "best_fp": 18,
                                "best_fn": 8,
                                "best_tn": 100,
                            },
                        }
                    ]
                ),
                encoding="utf-8",
            )

            rows = rows_from_summary(path, "baseline_summary")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["algorithm"], "logistic_regression")
        self.assertEqual(rows[0]["model"], "metrics_only")
        self.assertEqual(rows[0]["f1"], 0.1333)

    def test_rows_from_fusion_logs_parses_ratio_and_threshold_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            (log_dir / "train_fusion_20260519_000000.log").write_text(
                "\n".join(
                    [
                        "negative_positive_ratio=50",
                        'selected_logs_metrics_graph logistic_regression threshold: {"best_threshold": 0.99, "best_precision": 0.0779, "best_recall": 0.2, "best_f1": 0.1121, "best_tp": 6, "best_fp": 71, "best_fn": 24, "best_tn": 133035}',
                    ]
                ),
                encoding="utf-8",
            )

            rows = rows_from_fusion_logs(log_dir)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["negative_positive_ratio"], "50")
        self.assertEqual(rows[0]["model"], "selected_logs_metrics_graph")
        self.assertEqual(rows[0]["tp"], 6)


if __name__ == "__main__":
    unittest.main()
