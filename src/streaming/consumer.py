import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from kafka import KafkaConsumer


ROOT_DIR = Path(__file__).resolve().parents[2]

if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.database.connection import (  # noqa: E402
    save_fraud_alert,
    save_prediction_history,
    setup_database_indexes,
)
from src.models.predict_fraud import predict_transaction  # noqa: E402


def decode_message(value: bytes) -> Dict[str, Any]:
    return json.loads(value.decode("utf-8"))


def create_consumer(
    topic_name: str,
    bootstrap_server: str,
    group_id: str,
) -> KafkaConsumer:
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_server,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=decode_message,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
    )


def consume_transactions(
    topic_name: str,
    bootstrap_server: str,
    group_id: str,
    threshold: float,
    limit: Optional[int],
) -> None:
    setup_database_indexes()

    consumer = create_consumer(
        topic_name=topic_name,
        bootstrap_server=bootstrap_server,
        group_id=group_id,
    )

    total_processed = 0
    total_alerts = 0

    print("Starting fraud detection consumer...")
    print(f"Kafka topic      : {topic_name}")
    print(f"Bootstrap server : {bootstrap_server}")
    print(f"Consumer group   : {group_id}")
    print(f"Alert threshold  : {threshold}")
    print(f"Limit            : {limit}")
    print("-" * 80)

    try:
        for message in consumer:
            transaction = message.value
            prediction_result = predict_transaction(
                transaction=transaction,
                threshold=threshold,
            )

            total_processed += 1

            history_record = transaction.copy()
            history_record.update(prediction_result)
            history_record["kafka_partition"] = message.partition
            history_record["kafka_offset"] = message.offset

            save_prediction_history(history_record)

            if prediction_result["prediction"] == 1:
                total_alerts += 1

                alert_record = {
                    "transaction_id": transaction.get("transaction_id"),
                    "transaction_type": transaction.get("type"),
                    "amount": transaction.get("amount"),
                    "nameOrig": transaction.get("nameOrig"),
                    "nameDest": transaction.get("nameDest"),
                    "actual_isFraud": transaction.get("isFraud"),
                    "prediction": prediction_result["prediction"],
                    "fraud_probability": prediction_result["fraud_probability"],
                    "threshold": prediction_result["threshold"],
                    "model_used": prediction_result["model_used"],
                    "kafka_partition": message.partition,
                    "kafka_offset": message.offset,
                }

                save_fraud_alert(alert_record)

                print(
                    f"FRAUD ALERT | transaction_id={transaction.get('transaction_id')} "
                    f"| amount={transaction.get('amount')} "
                    f"| probability={prediction_result['fraud_probability']}"
                )

            if total_processed % 50 == 0:
                print(
                    f"Processed={total_processed} | Alerts={total_alerts}"
                )

            if limit is not None and total_processed >= limit:
                break

    finally:
        consumer.close()

    print("-" * 80)
    print("Consumer stopped.")
    print(f"Total processed: {total_processed}")
    print(f"Total alerts   : {total_alerts}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kafka consumer for real-time fraud prediction"
    )

    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--group-id", default="fraud-detection-consumer")
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--limit", type=int, default=100)

    args = parser.parse_args()

    consume_transactions(
        topic_name=args.topic,
        bootstrap_server=args.bootstrap_server,
        group_id=args.group_id,
        threshold=args.threshold,
        limit=args.limit,
    )