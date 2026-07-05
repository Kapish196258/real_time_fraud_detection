import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any

import pandas as pd
from kafka import KafkaProducer


def convert_value(value: Any) -> Any:
    """
    Converts pandas/numpy values into JSON-safe Python values.
    Kafka messages are sent as JSON, so all values must be serializable.
    """
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


def create_producer(bootstrap_server: str) -> KafkaProducer:
    """
    Creates Kafka producer connection.

    Since this Python script runs on Windows and Kafka runs inside Docker,
    we connect using localhost:9092.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
        linger_ms=10,
    )


def stream_transactions(
    file_path: str,
    topic_name: str,
    bootstrap_server: str,
    chunk_size: int,
    limit: Optional[int],
    sleep_time: float,
) -> None:
    """
    Reads PaySim transactions in chunks and streams them into Kafka.

    The dataset has more than 6 million rows, so chunk processing is used
    instead of loading the full CSV into memory at once.
    """

    dataset_path = Path(file_path)

    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found at: {dataset_path}")

    producer = create_producer(bootstrap_server)

    total_sent = 0

    print("Starting transaction streaming...")
    print(f"Dataset path      : {dataset_path}")
    print(f"Kafka topic       : {topic_name}")
    print(f"Bootstrap server  : {bootstrap_server}")
    print(f"Chunk size        : {chunk_size}")
    print(f"Limit             : {limit}")
    print(f"Sleep time        : {sleep_time} seconds")
    print("-" * 70)

    try:
        for chunk in pd.read_csv(dataset_path, chunksize=chunk_size):
            records = chunk.to_dict(orient="records")

            for record in records:
                total_sent += 1

                transaction = {
                    key: convert_value(value)
                    for key, value in record.items()
                }

                transaction["transaction_id"] = total_sent
                transaction["stream_timestamp"] = datetime.now(timezone.utc).isoformat()

                producer.send(
                    topic_name,
                    key=transaction["transaction_id"],
                    value=transaction,
                )

                if total_sent % 100 == 0:
                    print(f"Sent {total_sent} transactions...")

                if sleep_time > 0:
                    time.sleep(sleep_time)

                if limit is not None and total_sent >= limit:
                    producer.flush()
                    print("-" * 70)
                    print(f"Streaming completed. Total transactions sent: {total_sent}")
                    return

        producer.flush()
        print("-" * 70)
        print(f"Streaming completed. Total transactions sent: {total_sent}")

    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kafka producer for streaming PaySim transaction data"
    )

    parser.add_argument(
        "--file-path",
        default="data/raw/paysim_transactions.csv",
        help="Path to PaySim transaction CSV file",
    )

    parser.add_argument(
        "--topic",
        default="transactions",
        help="Kafka topic name",
    )

    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server for local producer",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50000,
        help="Number of rows to read at one time",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of transactions to stream for testing",
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.05,
        help="Delay between messages to simulate real-time streaming",
    )

    args = parser.parse_args()

    stream_transactions(
        file_path=args.file_path,
        topic_name=args.topic,
        bootstrap_server=args.bootstrap_server,
        chunk_size=args.chunk_size,
        limit=args.limit,
        sleep_time=args.sleep,
    )