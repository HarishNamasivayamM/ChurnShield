import csv
import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from kafka_producer_churn import load_records, parse_args


class ProducerTests(unittest.TestCase):
    def make_csv(self, rows: list[dict[str, str]]) -> Path:
        with tempfile.NamedTemporaryFile(
            mode="w", newline="", encoding="utf-8", suffix=".csv", delete=False
        ) as handle:
            path = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return path

    def test_load_records_normalises_numeric_and_blank_values(self):
        row = {
            "customerID": "A-1",
            "gender": " Female ",
            "SeniorCitizen": "0",
            "Partner": "Yes",
            "Dependents": "No",
            "tenure": "1",
            "PhoneService": "Yes",
            "MultipleLines": "No",
            "InternetService": "DSL",
            "OnlineSecurity": "No",
            "OnlineBackup": "Yes",
            "DeviceProtection": "No",
            "TechSupport": "No",
            "StreamingTV": "No",
            "StreamingMovies": "No",
            "Contract": "Month-to-month",
            "PaperlessBilling": "Yes",
            "PaymentMethod": "Electronic check",
            "MonthlyCharges": "29.85",
            "TotalCharges": " ",
            "Churn": "No",
        }
        records = list(load_records(self.make_csv([row])))
        self.assertEqual(records[0]["customerID"], "A-1")
        self.assertEqual(records[0]["gender"], "Female")
        self.assertEqual(records[0]["SeniorCitizen"], 0)
        self.assertEqual(records[0]["tenure"], 1)
        self.assertEqual(records[0]["MonthlyCharges"], 29.85)
        self.assertIsNone(records[0]["TotalCharges"])

    def test_parse_args_rejects_negative_rate(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse_args(["--rate", "-1"])


if __name__ == "__main__":
    unittest.main()
