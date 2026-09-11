"""Load scored prediction CSV rows into a Snowflake table."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

from churnstream.modeling import PREDICTION_COLUMNS

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("artifacts/predictions.csv"))
    parser.add_argument(
        "--table", default="CHURN_ANALYSIS.PREDICTIONS.CHURN_PREDICTIONS"
    )
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate input without connecting"
    )
    return parser.parse_args(argv)


def _quoted_table(name: str) -> str:
    parts = name.split(".")
    if len(parts) not in (2, 3) or any(
        not IDENTIFIER.fullmatch(part) for part in parts
    ):
        raise ValueError(
            "--table must be TABLE, SCHEMA.TABLE, or DATABASE.SCHEMA.TABLE "
            "using simple SQL identifiers"
        )
    return ".".join(f'"{part.upper()}"' for part in parts)


def _load_rows(path: Path) -> list[tuple[object, ...]]:
    frame = pd.read_csv(path)
    missing = sorted(set(PREDICTION_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(
            "Prediction file is missing required columns: " + ", ".join(missing)
        )
    if frame.empty:
        return []

    frame = frame[PREDICTION_COLUMNS].astype(object)
    frame = frame.where(pd.notna(frame), None)
    rows: list[tuple[object, ...]] = []
    for record in frame.to_dict(orient="records"):
        timestamp = record["PREDICTION_TS"]
        if timestamp is not None:
            timestamp = (
                pd.to_datetime(timestamp, utc=True).tz_localize(None).to_pydatetime()
            )
        risk_factors = record["RISK_FACTORS"]
        if risk_factors is not None:
            json.loads(str(risk_factors))
            risk_factors = str(risk_factors)
        rows.append(
            (
                record["CUSTOMERID"],
                timestamp,
                record["CHURN_FLAG"],
                record["CHURN_PROBABILITY"],
                record["RISK_LEVEL"],
                risk_factors,
                record["TENURE"],
                record["MONTHLYCHARGES"],
                record["TOTALCHARGES"],
                record["CONTRACT"],
                record["ACTUAL_CHURN_FLAG"],
            )
        )
    return rows


def run(args: argparse.Namespace) -> int:
    if args.batch_size < 1:
        raise ValueError("--batch-size must be at least 1")
    table = _quoted_table(args.table)
    rows = _load_rows(args.input)
    if args.dry_run:
        print(f"Validated {len(rows):,} prediction row(s) for {table}.")
        return 0
    try:
        import snowflake.connector
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Snowflake support is optional. Run 'python -m pip install -r "
            "requirements-snowflake.txt' first."
        ) from exc

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_env = [name for name in required if not os.getenv(name)]
    if missing_env:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing_env))

    insert_sql = f"""
        INSERT INTO {table} (
            CUSTOMERID, PREDICTION_TS, CHURN_FLAG, CHURN_PROBABILITY,
            RISK_LEVEL, RISK_FACTORS, TENURE, MONTHLYCHARGES, TOTALCHARGES,
            CONTRACT, ACTUAL_CHURN_FLAG
        )
        SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s
    """
    connection = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CHURN_ANALYSIS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PREDICTIONS"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    try:
        with connection.cursor() as cursor:
            for start in range(0, len(rows), args.batch_size):
                cursor.executemany(insert_sql, rows[start : start + args.batch_size])
        connection.commit()
    finally:
        connection.close()
    print(f"Loaded {len(rows):,} prediction row(s) into {table}.")
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except (FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
