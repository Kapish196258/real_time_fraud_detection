import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report,
)

from sklearn.utils.class_weight import compute_sample_weight


ROOT_DIR = Path(__file__).resolve().parents[2]

DATA_PATH = ROOT_DIR / "data" / "processed" / "processed_transactions.csv"
MODEL_DIR = ROOT_DIR / "models" / "saved_model"

TARGET = "isFraud"

DROP_COLUMNS = [
    "transaction_id",
    "type",
    "isFlaggedFraud",
    "is_origin_customer",
    TARGET,
]


def convert_json_value(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def load_dataset() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Processed dataset not found at {DATA_PATH}. "
            "Run src/features/feature_engineering.py first."
        )

    print("Loading processed dataset...")
    df = pd.read_csv(DATA_PATH)

    print(f"Dataset shape: {df.shape}")
    return df


def prepare_features(df: pd.DataFrame):
    feature_columns = [col for col in df.columns if col not in DROP_COLUMNS]

    X = df[feature_columns]
    y = df[TARGET]

    X = X.replace([np.inf, -np.inf], np.nan).fillna(0)

    print(f"Feature shape: {X.shape}")
    print(f"Target shape: {y.shape}")
    print(f"Number of features: {len(feature_columns)}")

    return X, y, feature_columns


def split_dataset(X: pd.DataFrame, y: pd.Series):
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=42,
        stratify=y,
    )

    print(f"X_train: {X_train.shape}")
    print(f"X_test : {X_test.shape}")
    print(f"y_train: {y_train.shape}")
    print(f"y_test : {y_test.shape}")

    return X_train, X_test, y_train, y_test


def build_models():
    models = {
        "Logistic Regression": Pipeline([
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(
                class_weight="balanced",
                max_iter=2000,
                solver="saga",
                random_state=42,
            )),
        ]),

        "Decision Tree": DecisionTreeClassifier(
            max_depth=12,
            class_weight="balanced",
            random_state=42,
        ),

        "Random Forest": RandomForestClassifier(
            n_estimators=150,
            class_weight="balanced_subsample",
            random_state=42,
            n_jobs=-1,
        ),

        "HistGradientBoosting": HistGradientBoostingClassifier(
            max_iter=200,
            learning_rate=0.1,
            max_leaf_nodes=31,
            random_state=42,
        ),
    }

    return models


def train_and_evaluate_models(models, X_train, X_test, y_train, y_test):
    trained_models = {}
    results = {}
    confusion_matrices = {}

    sample_weights = compute_sample_weight(class_weight="balanced", y=y_train)

    for model_name, model in models.items():
        print("=" * 80)
        print(f"Training model: {model_name}")

        if model_name == "HistGradientBoosting":
            model.fit(X_train, y_train, sample_weight=sample_weights)
        else:
            model.fit(X_train, y_train)

        predictions = model.predict(X_test)

        cm = confusion_matrix(y_test, predictions)
        tn, fp, fn, tp = cm.ravel()

        trained_models[model_name] = model
        confusion_matrices[model_name] = cm

        results[model_name] = {
            "Accuracy": accuracy_score(y_test, predictions),
            "Precision": precision_score(y_test, predictions, zero_division=0),
            "Recall": recall_score(y_test, predictions, zero_division=0),
            "F1 Score": f1_score(y_test, predictions, zero_division=0),
            "False Positives": fp,
            "False Negatives": fn,
            "True Positives": tp,
            "True Negatives": tn,
        }

        print(classification_report(y_test, predictions, zero_division=0))
        print("Confusion Matrix:")
        print(cm)

    results_df = pd.DataFrame(results).T

    results_df = results_df.sort_values(
        by=["False Negatives", "F1 Score", "Precision"],
        ascending=[True, False, False],
    )

    print("=" * 80)
    print("Final Model Comparison:")
    print(results_df)

    return trained_models, results_df, confusion_matrices


def save_model(best_model, best_model_name, feature_columns, results_df):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    model_path = MODEL_DIR / "fraud_detection_model.pkl"
    features_path = MODEL_DIR / "model_features.pkl"
    metadata_path = MODEL_DIR / "model_metadata.json"

    joblib.dump(best_model, model_path, compress=3)
    joblib.dump(feature_columns, features_path)

    metadata = {
        "best_model": best_model_name,
        "target": TARGET,
        "features": feature_columns,
        "dropped_columns": DROP_COLUMNS,
        "evaluation_results": results_df.to_dict(),
    }

    with open(metadata_path, "w") as file:
        json.dump(metadata, file, indent=4, default=convert_json_value)

    print("=" * 80)
    print(f"Best model: {best_model_name}")
    print(f"Model saved at: {model_path}")
    print(f"Feature list saved at: {features_path}")
    print(f"Metadata saved at: {metadata_path}")


def main():
    df = load_dataset()

    print("\nTarget distribution:")
    print(df[TARGET].value_counts())
    print("\nTarget percentage:")
    print(df[TARGET].value_counts(normalize=True) * 100)

    X, y, feature_columns = prepare_features(df)

    X_train, X_test, y_train, y_test = split_dataset(X, y)

    models = build_models()

    trained_models, results_df, _ = train_and_evaluate_models(
        models,
        X_train,
        X_test,
        y_train,
        y_test,
    )

    best_model_name = results_df.index[0]
    best_model = trained_models[best_model_name]

    save_model(best_model, best_model_name, feature_columns, results_df)


if __name__ == "__main__":
    main()