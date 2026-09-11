"""Consume Kafka customer events, score them, and append predictions to CSV."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from churnstream.modeling import REQUIRED_COLUMNS, load_bundle, score_records


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="customer_events")
    parser.add_argument("--model", type=Path, default=Path("artifacts/model.joblib"))
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/stream_predictions.csv")
    )
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--idle-timeout-ms", type=int, default=10_000)
    parser.add_argument("--group-id", default="churnstream-scorer")
    parser.add_argument("--from-beginning", action="store_true")
    return parser.parse_args(argv)


def _consumer(args: argparse.Namespace):
    try:
        from kafka import KafkaConsumer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "The Kafka client is not installed. Run 'python -m pip install -r requirements.txt'."
        ) from exc
    return KafkaConsumer(
        args.topic,
        bootstrap_servers=[item.strip() for item in args.bootstrap_servers.split(",")],
        group_id=args.group_id,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=args.idle_timeout_ms,
    )


def _prepare_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if "Churn" not in frame:
        frame["Churn"] = pd.NA
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(
            "Kafka messages are missing required fields: " + ", ".join(missing)
        )
    for column in ["SeniorCitizen", "tenure", "MonthlyCharges", "TotalCharges"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ["customerID", "Churn"]:
        frame[column] = frame[column].astype("string").str.strip()
    return frame


def _append_predictions(
    bundle: dict[str, object], rows: list[dict[str, object]], output: Path
) -> int:
    if not rows:
        return 0
    predictions = score_records(bundle, _prepare_frame(rows))
    output.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(output, mode="a", header=not output.exists(), index=False)
    return len(predictions)


def run(args: argparse.Namespace) -> int:
    if args.batch_size < 1:
        raise ValueError("--batch-size must be at least 1")
    if args.max_messages is not None and args.max_messages < 1:
        raise ValueError("--max-messages must be at least 1")
    bundle = load_bundle(args.model)
    consumer = _consumer(args)
    rows: list[dict[str, object]] = []
    scored_count = 0
    try:
        for message in consumer:
            if not isinstance(message.value, dict):
                continue
            rows.append(message.value)
            if (
                args.max_messages is not None
                and scored_count + len(rows) >= args.max_messages
            ):
                remaining = args.max_messages - scored_count
                if remaining > 0:
                    scored_count += _append_predictions(
                        bundle, rows[:remaining], args.output
                    )
                rows.clear()
                break
            if len(rows) >= args.batch_size:
                scored_count += _append_predictions(bundle, rows, args.output)
                rows.clear()
    except StopIteration:
        pass
    finally:
        consumer.close()

    if not rows and scored_count == 0:
        print("No Kafka messages received before the idle timeout.")
        return 0
    scored_count += _append_predictions(bundle, rows, args.output)
    print(f"Scored {scored_count} Kafka message(s) to {args.output}.")
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except (FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
