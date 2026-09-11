"""Train the churn model and write metrics, model, and scored predictions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from churnstream.modeling import (
    FEATURE_COLUMNS,
    load_dataset,
    save_bundle,
    score_records,
    train_model,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("churn_dataset.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--estimators", type=int, default=250)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.estimators < 10:
        raise SystemExit("--estimators must be at least 10")
    frame = load_dataset(args.input)
    model, metrics = train_model(
        frame, random_state=args.seed, n_estimators=args.estimators
    )
    model_path = args.output_dir / "model.joblib"
    metrics_path = args.output_dir / "metrics.json"
    predictions_path = args.output_dir / "predictions.csv"
    save_bundle(model, metrics, model_path)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    bundle = {
        "model": model,
        "metrics": metrics,
        "feature_columns": FEATURE_COLUMNS,
    }
    score_records(bundle, frame).to_csv(predictions_path, index=False)
    print(
        json.dumps(
            {k: v for k, v in metrics.items() if k != "feature_importance"}, indent=2
        )
    )
    print(f"Wrote model: {model_path}")
    print(f"Wrote metrics: {metrics_path}")
    print(f"Wrote predictions: {predictions_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
