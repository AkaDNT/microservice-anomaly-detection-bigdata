import argparse
import csv
import json
import time
from pathlib import Path

DEFAULT_INPUT = (
    "data/raw/train-ticket/case_01_admin_basic_info_spring_1_5_22/"
    "LOGS_ts-admin-basic-info-service_springstarterweb_1.5.22.RELEASE.txt_structured.csv"
)


def infer_service_name(path: Path) -> str:
    name = path.name
    if name.startswith("LOGS_"):
        name = name[len("LOGS_") :]
    marker = ".txt_structured.csv"
    if name.endswith(marker):
        name = name[: -len(marker)]
    return name.split("_")[0]


def build_event(row: dict, service_name: str, index: int) -> dict:
    return {
        "event_index": index,
        "timestamp": f"{row.get('Date', '')} {row.get('Time', '')}".strip(),
        "service_name": service_name,
        "level": (row.get("Level") or "").upper(),
        "event_id": row.get("EventId") or "",
        "event_template": row.get("EventTemplate") or "",
        "content": row.get("Content") or "",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay structured Train-Ticket logs into a Kafka topic.")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="train-ticket-logs")
    parser.add_argument("--service-name", default=None)
    parser.add_argument("--limit", type=int, default=0, help="Max rows to send. Use 0 for all rows.")
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true", help="Print events without sending to Kafka.")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Missing input log file: {input_path}")

    service_name = args.service_name or infer_service_name(input_path)
    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

    sent = 0
    with input_path.open("r", encoding="utf-8", errors="ignore", newline="") as file:
        reader = csv.DictReader(file)
        for index, row in enumerate(reader):
            if args.limit > 0 and sent >= args.limit:
                break
            event = build_event(row, service_name, index)
            if args.dry_run:
                print(json.dumps(event, ensure_ascii=False))
            else:
                assert producer is not None
                producer.send(args.topic, event)
            sent += 1
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

    if producer is not None:
        producer.flush()
        producer.close()

    print(f"sent={sent} topic={args.topic} service_name={service_name}")


if __name__ == "__main__":
    main()
