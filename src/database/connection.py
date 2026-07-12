"""MongoDB connection and persistence utilities.

This module provides reusable helpers for:

1. Connecting to the fraud-detection MongoDB database.
2. Preparing indexes for prediction and alert collections.
3. Saving one prediction result.
4. Saving one fraud alert.
5. Saving a complete Spark micro-batch using idempotent upserts.

The batch helper uses transaction_id as the lookup key. This allows Spark
to retry a micro-batch without creating another copy of the same transaction.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.results import UpdateResult


MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/",
)

DATABASE_NAME = os.getenv(
    "MONGO_DATABASE",
    "fraud_detection_db",
)

PREDICTION_HISTORY_COLLECTION = "prediction_history"
FRAUD_ALERTS_COLLECTION = "fraud_alerts"

_client: MongoClient | None = None


def get_mongo_client() -> MongoClient:
    """Create or return the shared MongoDB client."""

    global _client

    if _client is None:
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
        )

    return _client


def get_database():
    """Return the project MongoDB database."""

    client = get_mongo_client()
    return client[DATABASE_NAME]


def get_prediction_history_collection() -> Collection:
    """Return the prediction-history collection."""

    database = get_database()
    return database[PREDICTION_HISTORY_COLLECTION]


def get_fraud_alerts_collection() -> Collection:
    """Return the fraud-alert collection."""

    database = get_database()
    return database[FRAUD_ALERTS_COLLECTION]


def setup_database_indexes() -> None:
    """Create indexes used by prediction and alert queries."""

    prediction_history = get_prediction_history_collection()
    fraud_alerts = get_fraud_alerts_collection()

    prediction_history.create_index(
        [("transaction_id", ASCENDING)]
    )
    prediction_history.create_index(
        [("prediction", ASCENDING)]
    )
    prediction_history.create_index(
        [("fraud_probability", DESCENDING)]
    )
    prediction_history.create_index(
        [("spark_batch_id", ASCENDING)]
    )
    prediction_history.create_index(
        [("processed_at", DESCENDING)]
    )

    fraud_alerts.create_index(
        [("transaction_id", ASCENDING)]
    )
    fraud_alerts.create_index(
        [("fraud_probability", DESCENDING)]
    )
    fraud_alerts.create_index(
        [("alert_status", ASCENDING)]
    )
    fraud_alerts.create_index(
        [("spark_batch_id", ASCENDING)]
    )
    fraud_alerts.create_index(
        [("created_at", DESCENDING)]
    )


def validate_transaction_id(record: dict[str, Any]) -> int:
    """Return a validated integer transaction ID."""

    transaction_id = record.get("transaction_id")

    if transaction_id is None:
        raise ValueError(
            "MongoDB record does not contain transaction_id."
        )

    return int(transaction_id)


def prepare_prediction_document(
    record: dict[str, Any],
    spark_batch_id: int | None = None,
) -> dict[str, Any]:
    """Prepare one prediction document for MongoDB."""

    transaction_id = validate_transaction_id(record)

    document = record.copy()
    document["transaction_id"] = transaction_id

    if spark_batch_id is not None:
        document["spark_batch_id"] = int(spark_batch_id)

    document["processed_at"] = datetime.now(timezone.utc)

    return document


def prepare_alert_document(
    record: dict[str, Any],
    spark_batch_id: int | None = None,
) -> dict[str, Any]:
    """Prepare one fraud-alert document for MongoDB."""

    document = prepare_prediction_document(
        record=record,
        spark_batch_id=spark_batch_id,
    )

    document["alert_status"] = "OPEN"
    document["updated_at"] = datetime.now(timezone.utc)

    return document


def save_prediction_history(
    record: dict[str, Any],
    spark_batch_id: int | None = None,
) -> UpdateResult:
    """Insert or update one prediction-history document."""

    collection = get_prediction_history_collection()

    document = prepare_prediction_document(
        record=record,
        spark_batch_id=spark_batch_id,
    )

    transaction_id = document["transaction_id"]
    created_at = datetime.now(timezone.utc)

    return collection.update_one(
        {"transaction_id": transaction_id},
        {
            "$set": document,
            "$setOnInsert": {
                "created_at": created_at,
            },
        },
        upsert=True,
    )


def save_fraud_alert(
    alert: dict[str, Any],
    spark_batch_id: int | None = None,
) -> UpdateResult:
    """Insert or update one fraud-alert document."""

    collection = get_fraud_alerts_collection()

    document = prepare_alert_document(
        record=alert,
        spark_batch_id=spark_batch_id,
    )

    transaction_id = document["transaction_id"]
    created_at = datetime.now(timezone.utc)

    return collection.update_one(
        {"transaction_id": transaction_id},
        {
            "$set": document,
            "$setOnInsert": {
                "created_at": created_at,
            },
        },
        upsert=True,
    )


def save_prediction_batch(
    prediction_records: list[dict[str, Any]],
    spark_batch_id: int,
) -> dict[str, int]:
    """Save one Spark prediction micro-batch.

    Every prediction is upserted into prediction_history.

    Records where prediction == 1 are also upserted into fraud_alerts.

    Upserts make the operation idempotent for Spark retries because an
    existing transaction_id is updated instead of inserted again.
    """

    if not prediction_records:
        return {
            "prediction_operations": 0,
            "prediction_upserts": 0,
            "prediction_matches": 0,
            "prediction_modifications": 0,
            "alert_operations": 0,
            "alert_upserts": 0,
            "alert_matches": 0,
            "alert_modifications": 0,
        }

    prediction_collection = (
        get_prediction_history_collection()
    )

    alert_collection = get_fraud_alerts_collection()

    prediction_operations: list[UpdateOne] = []
    alert_operations: list[UpdateOne] = []

    operation_time = datetime.now(timezone.utc)

    for record in prediction_records:
        prediction_document = prepare_prediction_document(
            record=record,
            spark_batch_id=spark_batch_id,
        )

        transaction_id = prediction_document[
            "transaction_id"
        ]

        prediction_operations.append(
            UpdateOne(
                {"transaction_id": transaction_id},
                {
                    "$set": prediction_document,
                    "$setOnInsert": {
                        "created_at": operation_time,
                    },
                },
                upsert=True,
            )
        )

        if int(prediction_document["prediction"]) == 1:
            alert_document = prepare_alert_document(
                record=record,
                spark_batch_id=spark_batch_id,
            )

            alert_operations.append(
                UpdateOne(
                    {"transaction_id": transaction_id},
                    {
                        "$set": alert_document,
                        "$setOnInsert": {
                            "created_at": operation_time,
                        },
                    },
                    upsert=True,
                )
            )

    prediction_result = prediction_collection.bulk_write(
        prediction_operations,
        ordered=False,
    )

    alert_upserts = 0
    alert_matches = 0
    alert_modifications = 0

    if alert_operations:
        alert_result = alert_collection.bulk_write(
            alert_operations,
            ordered=False,
        )

        alert_upserts = len(
            alert_result.upserted_ids
        )
        alert_matches = alert_result.matched_count
        alert_modifications = (
            alert_result.modified_count
        )

    return {
        "prediction_operations": len(
            prediction_operations
        ),
        "prediction_upserts": len(
            prediction_result.upserted_ids
        ),
        "prediction_matches": (
            prediction_result.matched_count
        ),
        "prediction_modifications": (
            prediction_result.modified_count
        ),
        "alert_operations": len(
            alert_operations
        ),
        "alert_upserts": alert_upserts,
        "alert_matches": alert_matches,
        "alert_modifications": alert_modifications,
    }


def test_connection() -> None:
    """Test MongoDB and create the required indexes."""

    client = get_mongo_client()
    client.admin.command("ping")

    setup_database_indexes()

    print("MongoDB connection successful.")
    print(f"Database ready: {DATABASE_NAME}")
    print(
        "Prediction collection:",
        PREDICTION_HISTORY_COLLECTION,
    )
    print(
        "Fraud-alert collection:",
        FRAUD_ALERTS_COLLECTION,
    )


def close_mongo_client() -> None:
    """Close the shared MongoDB client."""

    global _client

    if _client is not None:
        _client.close()
        _client = None


if __name__ == "__main__":
    test_connection()
    close_mongo_client()