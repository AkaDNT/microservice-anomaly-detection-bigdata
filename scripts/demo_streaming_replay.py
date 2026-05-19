import argparse
import csv
import json
import time
from pathlib import Path
from typing import Iterable


def iter_log_events(path: Path, limit: int) -> Iterable[dict]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as file:
        reader = csv.DictReader(file)
        for index, row in enumerate(reader):
            if limit > 0 and index >= limit:
                break
            yield {
                "event_index": index,
                "timestamp": f"{row.get('Date', '')} {row.get('Time', '')}".strip(),
                "level": row.get("Level", ""),
                "component": row.get("Component", ""),
                "event_id": row.get("EventId", ""),
                "event_template": row.get("EventTemplate", ""),
                "content": row.get("Content", ""),
            }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay structured log rows as JSONL streaming-demo events.")
    parser.add_argument(
        "--input",
        default="data/raw/train-ticket/case_01_admin_basic_info_spring_1_5_22/LOGS_ts-admin-basic-info-service_springstarterweb_1.5.22.RELEASE.txt_structured.csv",
    )
    parser.add_argument("--output", default="reports/dashboard/streaming_replay_sample.jsonl")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.exists():
        raise FileNotFoundError(f"Missing input log file: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for event in iter_log_events(input_path, args.limit):
            file.write(json.dumps(event, ensure_ascii=False) + "\n")
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

    print(f"wrote {output_path}")


if __name__ == "__main__":
    main()
