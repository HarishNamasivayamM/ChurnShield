"""Data loading, model training, scoring, and risk explanations."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

ID_COLUMN = "customerID"
TARGET_COLUMN = "Churn"
NUMERIC_COLUMNS = ["SeniorCitizen", "tenure", "MonthlyCharges", "TotalCharges"]
CATEGORICAL_COLUMNS = [
    "gender",
    "Partner",
    "Dependents",
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
]
FEATURE_COLUMNS = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS
REQUIRED_COLUMNS = {ID_COLUMN, TARGET_COLUMN, *FEATURE_COLUMNS}
PREDICTION_COLUMNS = [
    "CUSTOMERID",
    "PREDICTION_TS",
    "CHURN_FLAG",
    "CHURN_PROBABILITY",
    "RISK_LEVEL",
    "RISK_FACTORS",
    "TENURE",
    "MONTHLYCHARGES",
    "TOTALCHARGES",
    "CONTRACT",
    "ACTUAL_CHURN_FLAG",
]


def load_dataset(path: Path) -> pd.DataFrame:
    """Load and validate a Telco churn CSV."""

    frame = pd.read_csv(path)
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"Dataset is missing required columns: {', '.join(missing)}")
    if frame.empty:
        raise ValueError("Dataset is empty")

    frame = frame.copy()
    for column in CATEGORICAL_COLUMNS + [ID_COLUMN, TARGET_COLUMN]:
        frame[column] = frame[column].astype("string").str.strip()
    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if frame[ID_COLUMN].isna().any() or (frame[ID_COLUMN] == "").any():
        raise ValueError("Dataset contains an empty customerID")
    if not frame[ID_COLUMN].is_unique:
        raise ValueError("customerID values must be unique")
    unknown_labels = sorted(set(frame[TARGET_COLUMN].dropna()) - {"Yes", "No"})
    if unknown_labels:
        raise ValueError(f"Churn contains unsupported labels: {unknown_labels}")
    return frame


def build_model(random_state: int = 42, n_estimators: int = 250) -> Pipeline:
    """Build the preprocessing and random-forest classification pipeline."""

    numeric_pipeline = Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))])
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, NUMERIC_COLUMNS),
            ("cat", categorical_pipeline, CATEGORICAL_COLUMNS),
        ],
        remainder="drop",
    )
    classifier = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=16,
        min_samples_leaf=2,
        class_weight="balanced_subsample",
        n_jobs=-1,
        random_state=random_state,
    )
    return Pipeline(steps=[("preprocessor", preprocessor), ("classifier", classifier)])


def _aggregate_feature_importance(model: Pipeline) -> dict[str, float]:
    preprocessor = model.named_steps["preprocessor"]
    classifier = model.named_steps["classifier"]
    names = preprocessor.get_feature_names_out()
    importances = classifier.feature_importances_
    grouped = {column: 0.0 for column in FEATURE_COLUMNS}
    for name, importance in zip(names, importances):
        short_name = name.split("__", 1)[-1]
        if short_name in NUMERIC_COLUMNS:
            grouped[short_name] += float(importance)
            continue
        for column in CATEGORICAL_COLUMNS:
            if short_name.startswith(f"{column}_"):
                grouped[column] += float(importance)
                break
    total = sum(grouped.values()) or 1.0
    return {column: value / total for column, value in grouped.items()}


def train_model(
    frame: pd.DataFrame,
    random_state: int = 42,
    test_size: float = 0.2,
    n_estimators: int = 250,
) -> tuple[Pipeline, dict[str, Any]]:
    """Train the model and return it with holdout metrics."""

    labels = frame[TARGET_COLUMN].map({"No": 0, "Yes": 1}).astype(int)
    features = frame[FEATURE_COLUMNS]
    x_train, x_test, y_train, y_test = train_test_split(
        features,
        labels,
        test_size=test_size,
        random_state=random_state,
        stratify=labels,
    )
    model = build_model(random_state=random_state, n_estimators=n_estimators)
    model.fit(x_train, y_train)
    probabilities = model.predict_proba(x_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    metrics: dict[str, Any] = {
        "model": "random_forest",
        "random_state": random_state,
        "n_estimators": n_estimators,
        "training_rows": len(x_train),
        "test_rows": len(x_test),
        "accuracy": float(accuracy_score(y_test, predictions)),
        "precision": float(precision_score(y_test, predictions, zero_division=0)),
        "recall": float(recall_score(y_test, predictions, zero_division=0)),
        "f1": float(f1_score(y_test, predictions, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_test, probabilities)),
        "feature_importance": _aggregate_feature_importance(model),
    }
    return model, metrics


def save_bundle(model: Pipeline, metrics: dict[str, Any], path: Path) -> None:
    """Persist the model and metadata together."""

    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model": model,
            "metrics": metrics,
            "feature_columns": FEATURE_COLUMNS,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        path,
    )


def load_bundle(path: Path) -> dict[str, Any]:
    bundle = joblib.load(path)
    if not isinstance(bundle, dict) or "model" not in bundle:
        raise ValueError(f"Invalid model bundle: {path}")
    if bundle.get("feature_columns") != FEATURE_COLUMNS:
        raise ValueError("Model bundle feature columns do not match this pipeline")
    return bundle


def _risk_factors(row: pd.Series, importance: dict[str, float]) -> str:
    candidates: list[tuple[str, str, float]] = []

    def add(label: str, feature: str, condition: bool) -> None:
        if condition:
            candidates.append((label, feature, importance.get(feature, 0.0)))

    add("Month-to-month contract", "Contract", row.get("Contract") == "Month-to-month")
    add("Short tenure", "tenure", pd.notna(row.get("tenure")) and row["tenure"] <= 6)
    add(
        "High monthly charges",
        "MonthlyCharges",
        pd.notna(row.get("MonthlyCharges")) and row["MonthlyCharges"] >= 80,
    )
    add("No online security", "OnlineSecurity", row.get("OnlineSecurity") == "No")
    add("No tech support", "TechSupport", row.get("TechSupport") == "No")
    add(
        "Electronic check payment",
        "PaymentMethod",
        row.get("PaymentMethod") == "Electronic check",
    )
    add("Senior citizen", "SeniorCitizen", row.get("SeniorCitizen") == 1)
    add("Missing total charges", "TotalCharges", pd.isna(row.get("TotalCharges")))
    candidates.sort(key=lambda item: (-item[2], item[0]))
    factors = [
        {"factor": label, "feature": feature, "importance": round(score, 6)}
        for label, feature, score in candidates[:3]
    ]
    return json.dumps(factors, separators=(",", ":"))


def score_records(bundle: dict[str, Any], frame: pd.DataFrame) -> pd.DataFrame:
    """Score customer records and return a Power BI/Snowflake-friendly table."""

    if frame.empty:
        return pd.DataFrame(columns=PREDICTION_COLUMNS)
    missing = sorted({ID_COLUMN, *FEATURE_COLUMNS} - set(frame.columns))
    if missing:
        raise ValueError(f"Records are missing required fields: {', '.join(missing)}")
    if frame[ID_COLUMN].isna().any() or (frame[ID_COLUMN].astype("string") == "").any():
        raise ValueError("Records contain an empty customerID")
    if bundle.get("feature_columns") != FEATURE_COLUMNS:
        raise ValueError("Model bundle feature columns do not match this pipeline")
    model: Pipeline = bundle["model"]
    probabilities = model.predict_proba(frame[FEATURE_COLUMNS])[:, 1]
    importance = bundle.get("metrics", {}).get("feature_importance", {})
    output = pd.DataFrame(
        {
            "CUSTOMERID": frame[ID_COLUMN].astype(str).to_numpy(),
            "PREDICTION_TS": datetime.now(timezone.utc).isoformat(),
            "CHURN_FLAG": (probabilities >= 0.5).astype(int),
            "CHURN_PROBABILITY": probabilities,
            "RISK_LEVEL": np.select(
                [probabilities >= 0.7, probabilities >= 0.4],
                ["High", "Medium"],
                default="Low",
            ),
            "RISK_FACTORS": [
                _risk_factors(row, importance) for _, row in frame.iterrows()
            ],
            "TENURE": frame["tenure"].to_numpy(),
            "MONTHLYCHARGES": frame["MonthlyCharges"].to_numpy(),
            "TOTALCHARGES": frame["TotalCharges"].to_numpy(),
            "CONTRACT": frame["Contract"].astype("string").to_numpy(),
            "ACTUAL_CHURN_FLAG": (
                frame[TARGET_COLUMN].map({"No": 0, "Yes": 1}).to_numpy()
                if TARGET_COLUMN in frame
                else np.full(len(frame), np.nan)
            ),
        }
    )
    return output[PREDICTION_COLUMNS]
