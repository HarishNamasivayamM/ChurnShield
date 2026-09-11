"""Publish rows from the bundled churn dataset to Kafka.

The producer is intentionally dependency-light: CSV parsing and value
normalisation use the Python standard library. Only the Kafka client is
loaded when a real Kafka connection is requested, which keeps ``--dry-run``
useful in environments where Kafka is not running yet.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections.abc import Iterator
from pathlib import Path

DEFAULT_CSV = Path(__file__).with_name("churn_dataset.csv")
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "customer_events")

REQUIRED_COLUMNS = {
    "customerID",
    "gender",
    "SeniorCitizen",
    "Partner",
    "Dependents",
    "tenure",
    "PhoneService",
    "MultipleLines",
    "InternetService",
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
    "Contract",
    "PaperlessBilling",
    "PaymentMethod",
    "MonthlyCharges",
    "TotalCharges",
    "Churn",
}
INTEGER_COLUMNS = {"SeniorCitizen", "tenure"}
FLOAT_COLUMNS = {"MonthlyCharges", "TotalCharges"}


def _normalise_value(column: str, value: str | None) -> object:
    """Return a JSON-safe, typed value for one CSV field."""
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    if column in INTEGER_COLUMNS:
        return int(value)
    if column in FLOAT_COLUMNS:
        return float(value)
    return value


def load_records(csv_path: Path) -> Iterator[dict[str, object]]:
    """Read and validate customer rows from ``csv_path``.

    The source dataset contains eleven whitespace-only ``TotalCharges``
    values. They are converted to JSON ``null`` instead of non-standard
    ``NaN`` values.
    """
    with csv_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        columns = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_COLUMNS - columns)
        if missing:
            raise ValueError(f"CSV is missing required column(s): {', '.join(missing)}")

        for line_number, raw_row in enumerate(reader, start=2):
            if None in raw_row:
                raise ValueError(
                    f"CSV row {line_number} has more fields than its header"
                )
            record = {
                column: _normalise_value(column, raw_row.get(column))
                for column in reader.fieldnames or []
            }
            customer_id = record.get("customerID")
            if not isinstance(customer_id, str) or not customer_id:
                raise ValueError(f"CSV row {line_number} has an empty customerID")
            yield record


def build_producer(bootstrap_servers: str):
    """Create a Kafka producer, with an actionable missing-dependency error."""
    try:
        from kafka import KafkaProducer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "The Kafka client is not installed. Run "
            "'python -m pip install -r requirements.txt'."
        ) from exc

    return KafkaProducer(
        bootstrap_servers=[
            server.strip() for server in bootstrap_servers.split(",") if server.strip()
        ],
        acks="all",
        retries=5,
        request_timeout_ms=30_000,
        linger_ms=5,
        compression_type="gzip",
        client_id="churnstream-producer",
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8"),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish churn_dataset.csv rows to a Kafka topic."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"Input CSV path (default: {DEFAULT_CSV.name}).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Comma-separated Kafka brokers (default: {DEFAULT_BOOTSTRAP_SERVERS}).",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic (default: {DEFAULT_TOPIC}).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Maximum rows per second; 0 sends as fast as Kafka accepts them.",
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Maximum rows to publish per pass."
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Replay the CSV continuously until interrupted.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview records without connecting to Kafka.",
    )
    args = parser.parse_args(argv)
    if args.rate < 0:
        parser.error("--rate must be zero or greater")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    return args


def run(args: argparse.Namespace) -> int:
    if not args.csv.is_file():
        raise FileNotFoundError(f"Input CSV was not found: {args.csv}")

    if args.dry_run:
        preview: dict[str, object] | None = None
        count = 0
        for record in load_records(args.csv):
            if preview is None:
                preview = record
            count += 1
            if args.limit is not None and count >= args.limit:
                break
        print(f"Validated {count} record(s) from {args.csv}.")
        if preview is not None:
            print(json.dumps(preview, ensure_ascii=False, sort_keys=True))
        return 0

    producer = build_producer(args.bootstrap_servers)
    total_sent = 0
    delay = 1.0 / args.rate if args.rate else 0.0
    try:
        while True:
            sent_this_pass = 0
            for record in load_records(args.csv):
                if args.limit is not None and sent_this_pass >= args.limit:
                    break
                producer.send(args.topic, key=str(record["customerID"]), value=record)
                sent_this_pass += 1
                total_sent += 1
                if delay:
                    time.sleep(delay)
            producer.flush(timeout=30)
            print(
                f"Published {sent_this_pass} record(s) to '{args.topic}' ({total_sent} total)."
            )
            if not args.loop:
                break
    finally:
        producer.close(timeout=30)
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except KeyboardInterrupt:
        print("Stopped by user.", file=sys.stderr)
        return 130
    except (FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
