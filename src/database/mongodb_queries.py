from pathlib import Path
from pprint import pprint
import sys


ROOT_DIR = Path(__file__).resolve().parents[2]

if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.database.connection import (  # noqa: E402
    get_fraud_alerts_collection,
    get_prediction_history_collection,
)


def print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def show_total_predictions() -> None:
    prediction_history = get_prediction_history_collection()

    total_predictions = prediction_history.count_documents({})

    print_section("Total Prediction Records")
    print(f"Total prediction records stored: {total_predictions}")


def show_total_fraud_alerts() -> None:
    fraud_alerts = get_fraud_alerts_collection()

    total_alerts = fraud_alerts.count_documents({})

    print_section("Total Fraud Alerts")
    print(f"Total fraud alerts stored: {total_alerts}")


def show_latest_predictions(limit: int = 5) -> None:
    prediction_history = get_prediction_history_collection()

    latest_predictions = prediction_history.find(
        {},
        {
            "_id": 0,
            "transaction_id": 1,
            "type": 1,
            "amount": 1,
            "isFraud": 1,
            "prediction": 1,
            "fraud_probability": 1,
            "processed_at": 1,
        },
    ).sort("processed_at", -1).limit(limit)

    print_section(f"Latest {limit} Predictions")

    for record in latest_predictions:
        pprint(record)


def show_top_fraud_probabilities(limit: int = 10) -> None:
    prediction_history = get_prediction_history_collection()

    top_records = prediction_history.find(
        {},
        {
            "_id": 0,
            "transaction_id": 1,
            "type": 1,
            "amount": 1,
            "isFraud": 1,
            "prediction": 1,
            "fraud_probability": 1,
            "processed_at": 1,
        },
    ).sort("fraud_probability", -1).limit(limit)

    print_section(f"Top {limit} Highest Fraud Probability Transactions")

    for record in top_records:
        pprint(record)


def show_prediction_distribution() -> None:
    prediction_history = get_prediction_history_collection()

    pipeline = [
        {
            "$group": {
                "_id": "$prediction",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "_id": 1,
            }
        },
    ]

    results = list(prediction_history.aggregate(pipeline))

    print_section("Fraud vs Non-Fraud Prediction Distribution")

    for record in results:
        label = "Fraud" if record["_id"] == 1 else "Non-Fraud"
        print(f"{label}: {record['count']}")


def show_actual_vs_predicted() -> None:
    prediction_history = get_prediction_history_collection()

    pipeline = [
        {
            "$group": {
                "_id": {
                    "actual": "$isFraud",
                    "predicted": "$prediction",
                },
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "_id.actual": 1,
                "_id.predicted": 1,
            }
        },
    ]

    results = list(prediction_history.aggregate(pipeline))

    print_section("Actual vs Predicted Comparison")

    for record in results:
        actual = record["_id"]["actual"]
        predicted = record["_id"]["predicted"]
        count = record["count"]

        if actual == 0 and predicted == 0:
            label = "True Negative"
        elif actual == 0 and predicted == 1:
            label = "False Positive"
        elif actual == 1 and predicted == 0:
            label = "False Negative"
        elif actual == 1 and predicted == 1:
            label = "True Positive"
        else:
            label = "Unknown"

        print(
            f"Actual={actual}, Predicted={predicted} | "
            f"{label}: {count}"
        )


def show_fraud_alerts_by_transaction_type() -> None:
    fraud_alerts = get_fraud_alerts_collection()

    pipeline = [
        {
            "$group": {
                "_id": "$transaction_type",
                "alert_count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "alert_count": -1,
            }
        },
    ]

    results = list(fraud_alerts.aggregate(pipeline))

    print_section("Fraud Alerts by Transaction Type")

    if not results:
        print("No fraud alerts found.")
        return

    for record in results:
        print(f"{record['_id']}: {record['alert_count']}")


def show_fraud_alert_amount_summary() -> None:
    fraud_alerts = get_fraud_alerts_collection()

    pipeline = [
        {
            "$group": {
                "_id": None,
                "total_alerts": {"$sum": 1},
                "average_amount": {"$avg": "$amount"},
                "min_amount": {"$min": "$amount"},
                "max_amount": {"$max": "$amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "total_alerts": 1,
                "average_amount": {"$round": ["$average_amount", 2]},
                "min_amount": 1,
                "max_amount": 1,
            }
        },
    ]

    results = list(fraud_alerts.aggregate(pipeline))

    print_section("Fraud Alert Amount Summary")

    if not results:
        print("No fraud alerts found.")
        return

    pprint(results[0])


def show_false_negatives(limit: int = 10) -> None:
    prediction_history = get_prediction_history_collection()

    false_negatives = list(
        prediction_history.find(
            {
                "isFraud": 1,
                "prediction": 0,
            },
            {
                "_id": 0,
                "transaction_id": 1,
                "type": 1,
                "amount": 1,
                "fraud_probability": 1,
            },
        ).limit(limit)
    )

    print_section("False Negatives")

    if not false_negatives:
        print("No false negatives found.")
        return

    for record in false_negatives:
        pprint(record)


def show_fraud_probability_buckets() -> None:
    prediction_history = get_prediction_history_collection()

    pipeline = [
        {
            "$bucket": {
                "groupBy": "$fraud_probability",
                "boundaries": [0, 0.25, 0.5, 0.75, 1.01],
                "default": "unknown",
                "output": {
                    "count": {"$sum": 1},
                },
            }
        }
    ]

    results = list(prediction_history.aggregate(pipeline))

    print_section("Fraud Probability Buckets")

    for record in results:
        pprint(record)


def run_all_queries() -> None:
    show_total_predictions()
    show_total_fraud_alerts()
    show_latest_predictions(limit=5)
    show_top_fraud_probabilities(limit=10)
    show_prediction_distribution()
    show_actual_vs_predicted()
    show_fraud_alerts_by_transaction_type()
    show_fraud_alert_amount_summary()
    show_false_negatives(limit=10)
    show_fraud_probability_buckets()


if __name__ == "__main__":
    run_all_queries()