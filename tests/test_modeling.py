import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from churnstream.modeling import (
    FEATURE_COLUMNS,
    load_dataset,
    score_records,
    train_model,
)
from kafka_consumer_churn import _append_predictions


class ModelingTests(unittest.TestCase):
    def test_training_and_scoring_produce_prediction_contract(self):
        frame = load_dataset(Path("churn_dataset.csv")).head(400).copy()
        model, metrics = train_model(frame, n_estimators=20)
        bundle = {
            "model": model,
            "metrics": metrics,
            "feature_columns": FEATURE_COLUMNS,
        }
        predictions = score_records(bundle, frame.head(5))
        self.assertEqual(len(predictions), 5)
        self.assertEqual(
            predictions["CUSTOMERID"].tolist(), frame["customerID"].head(5).tolist()
        )
        self.assertTrue(predictions["CHURN_PROBABILITY"].between(0, 1).all())
        self.assertTrue(predictions["RISK_LEVEL"].isin(["Low", "Medium", "High"]).all())
        json.loads(predictions.iloc[0]["RISK_FACTORS"])

    def test_stream_batch_appends_scored_rows(self):
        frame = load_dataset(Path("churn_dataset.csv")).head(400).copy()
        model, metrics = train_model(frame, n_estimators=20)
        bundle = {
            "model": model,
            "metrics": metrics,
            "feature_columns": FEATURE_COLUMNS,
        }
        rows = frame.drop(columns=["Churn"]).head(3).to_dict(orient="records")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "stream_predictions.csv"
            self.assertEqual(_append_predictions(bundle, rows, output), 3)
            self.assertEqual(_append_predictions(bundle, rows, output), 3)
            saved = pd.read_csv(output)
            self.assertEqual(len(saved), 6)
            self.assertEqual(saved["CUSTOMERID"].nunique(), 3)


if __name__ == "__main__":
    unittest.main()
