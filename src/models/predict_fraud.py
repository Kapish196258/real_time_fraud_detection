from pathlib import Path
from typing import Any, Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]

MODEL_DIR = ROOT_DIR / "models" / "saved_model"
MODEL_PATH = MODEL_DIR / "fraud_detection_model.pkl"
FEATURES_PATH = MODEL_DIR / "model_features.pkl"


def load_model_artifacts() -> Tuple[Any, List[str]]:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model file not found at {MODEL_PATH}. "
            "Run src/models/train_model.py first."
        )

    if not FEATURES_PATH.exists():
        raise FileNotFoundError(
            f"Feature list file not found at {FEATURES_PATH}. "
            "Run src/models/train_model.py first."
        )

    model = joblib.load(MODEL_PATH)
    feature_columns = joblib.load(FEATURES_PATH)

    return model, feature_columns


def prepare_transaction_features(
    transaction: Dict[str, Any],
    feature_columns: List[str],
) -> pd.DataFrame:
    df = pd.DataFrame([transaction])

    numeric_columns = [
        "step",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    ]

    for column in numeric_columns:
        if column not in df.columns:
            df[column] = 0

        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    if "nameOrig" not in df.columns:
        df["nameOrig"] = ""

    if "nameDest" not in df.columns:
        df["nameDest"] = ""

    if "type" not in df.columns:
        df["type"] = "UNKNOWN"

    df["origin_prefix"] = df["nameOrig"].astype(str).str[0]
    df["dest_prefix"] = df["nameDest"].astype(str).str[0]

    df["is_origin_customer"] = (df["origin_prefix"] == "C").astype(int)
    df["is_dest_customer"] = (df["dest_prefix"] == "C").astype(int)
    df["is_dest_merchant"] = (df["dest_prefix"] == "M").astype(int)

    df["origin_balance_diff"] = df["oldbalanceOrg"] - df["newbalanceOrig"]
    df["destination_balance_diff"] = df["newbalanceDest"] - df["oldbalanceDest"]

    df["origin_balance_error"] = (
        df["oldbalanceOrg"] - df["amount"] - df["newbalanceOrig"]
    )

    df["destination_balance_error"] = (
        df["oldbalanceDest"] + df["amount"] - df["newbalanceDest"]
    )

    df["abs_origin_balance_error"] = df["origin_balance_error"].abs()
    df["abs_destination_balance_error"] = df["destination_balance_error"].abs()

    df["amount_to_oldbalanceOrg_ratio"] = df["amount"] / (df["oldbalanceOrg"] + 1)
    df["amount_to_oldbalanceDest_ratio"] = df["amount"] / (df["oldbalanceDest"] + 1)

    df["is_zero_oldbalanceOrg"] = (df["oldbalanceOrg"] == 0).astype(int)
    df["is_zero_oldbalanceDest"] = (df["oldbalanceDest"] == 0).astype(int)

    transaction_type_encoded = pd.get_dummies(df["type"], prefix="type", dtype=int)
    df = pd.concat([df, transaction_type_encoded], axis=1)

    for column in feature_columns:
        if column not in df.columns:
            df[column] = 0

    model_input = df[feature_columns]
    model_input = model_input.replace([np.inf, -np.inf], np.nan).fillna(0)

    return model_input


def predict_transaction(
    transaction: Dict[str, Any],
    threshold: float = 0.5,
) -> Dict[str, Any]:
    model, feature_columns = load_model_artifacts()

    model_input = prepare_transaction_features(transaction, feature_columns)

    if hasattr(model, "predict_proba"):
        fraud_probability = float(model.predict_proba(model_input)[0][1])
        prediction = int(fraud_probability >= threshold)
    else:
        prediction = int(model.predict(model_input)[0])
        fraud_probability = None

    return {
        "transaction_id": transaction.get("transaction_id"),
        "prediction": prediction,
        "fraud_probability": fraud_probability,
        "threshold": threshold,
        "model_used": type(model).__name__,
    }


if __name__ == "__main__":
    sample_transaction = {
        "transaction_id": 1,
        "step": 1,
        "type": "TRANSFER",
        "amount": 181.00,
        "nameOrig": "C1305486145",
        "oldbalanceOrg": 181.00,
        "newbalanceOrig": 0.00,
        "nameDest": "C553264065",
        "oldbalanceDest": 0.00,
        "newbalanceDest": 0.00,
        "isFraud": 1,
        "isFlaggedFraud": 0,
    }

    result = predict_transaction(sample_transaction)
    print(result)