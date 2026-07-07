import os
from datetime import datetime, timezone
from typing import Any, Dict

from pymongo import ASCENDING, DESCENDING, MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DATABASE_NAME = os.getenv("MONGO_DATABASE", "fraud_detection_db")

_client = None


def get_mongo_client() -> MongoClient:
    global _client

    if _client is None:
        _client = MongoClient(MONGO_URI)

    return _client


def get_database():
    client = get_mongo_client()
    return client[DATABASE_NAME]


def get_prediction_history_collection():
    db = get_database()
    return db["prediction_history"]


def get_fraud_alerts_collection():
    db = get_database()
    return db["fraud_alerts"]


def setup_database_indexes() -> None:
    prediction_history = get_prediction_history_collection()
    fraud_alerts = get_fraud_alerts_collection()

    prediction_history.create_index([("transaction_id", ASCENDING)])
    prediction_history.create_index([("prediction", ASCENDING)])
    prediction_history.create_index([("fraud_probability", DESCENDING)])
    prediction_history.create_index([("processed_at", DESCENDING)])

    fraud_alerts.create_index([("transaction_id", ASCENDING)])
    fraud_alerts.create_index([("fraud_probability", DESCENDING)])
    fraud_alerts.create_index([("alert_status", ASCENDING)])
    fraud_alerts.create_index([("created_at", DESCENDING)])


def save_prediction_history(record: Dict[str, Any]):
    prediction_history = get_prediction_history_collection()

    record = record.copy()
    record["processed_at"] = datetime.now(timezone.utc)

    return prediction_history.insert_one(record)


def save_fraud_alert(alert: Dict[str, Any]):
    fraud_alerts = get_fraud_alerts_collection()

    alert = alert.copy()
    alert["alert_status"] = "OPEN"
    alert["created_at"] = datetime.now(timezone.utc)

    return fraud_alerts.insert_one(alert)


def test_connection() -> None:
    client = get_mongo_client()
    client.admin.command("ping")

    setup_database_indexes()

    print("MongoDB connection successful.")
    print(f"Database ready: {DATABASE_NAME}")


if __name__ == "__main__":
    test_connection()