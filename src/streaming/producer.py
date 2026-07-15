import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from kafka import KafkaProducer


def convert_value(value: Any) -> Any:
    """
    Convert pandas and NumPy values into JSON-safe Python values.

    Kafka messages are sent as JSON, so every value must be serializable.
    Missing pandas values are converted to None.
    """

    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


def create_producer(bootstrap_server: str) -> KafkaProducer:
    """
    Create and return a Kafka producer connection.

    This Python script runs on Windows while Kafka runs inside Docker.
    Therefore, the producer connects through localhost:9092.
    """

    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            allow_nan=False,
        ).encode("utf-8"),
        retries=5,
        linger_ms=10,
        acks="all",
    )


def validate_arguments(
    start_row: int,
    chunk_size: int,
    limit: Optional[int],
    sleep_time: float,
) -> None:
    """
    Validate producer arguments before connecting to Kafka.
    """

    if start_row < 0:
        raise ValueError(
            "--start-row must be zero or greater"
        )

    if chunk_size <= 0:
        raise ValueError(
            "--chunk-size must be greater than zero"
        )

    if limit is not None and limit <= 0:
        raise ValueError(
            "--limit must be greater than zero"
        )

    if sleep_time < 0:
        raise ValueError(
            "--sleep must be zero or greater"
        )


def stream_transactions(
    file_path: str,
    topic_name: str,
    bootstrap_server: str,
    chunk_size: int,
    start_row: int,
    limit: Optional[int],
    sleep_time: float,
) -> None:
    """
    Read PaySim transactions in chunks and stream them into Kafka.

    The PaySim dataset has more than six million rows, so chunk-based
    processing is used instead of loading the complete CSV file into memory.

    start_row is zero-based.

    Examples:
        start_row=0, limit=5
        sends dataset rows 0-4 with transaction IDs 1-5.

        start_row=2000, limit=5
        sends dataset rows 2000-2004 with transaction IDs 2001-2005.
    """

    validate_arguments(
        start_row=start_row,
        chunk_size=chunk_size,
        limit=limit,
        sleep_time=sleep_time,
    )

    dataset_path = Path(file_path)

    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Dataset not found at: {dataset_path}"
        )

    producer = create_producer(
        bootstrap_server=bootstrap_server
    )

    total_sent = 0
    rows_seen = 0

    expected_start_id = start_row + 1

    expected_end_id = (
        start_row + limit
        if limit is not None
        else None
    )

    print("Starting transaction streaming...")
    print(f"Dataset path      : {dataset_path}")
    print(f"Kafka topic       : {topic_name}")
    print(f"Bootstrap server  : {bootstrap_server}")
    print(f"Chunk size        : {chunk_size}")
    print(f"Start row         : {start_row}")
    print(f"Limit             : {limit}")
    print(f"Sleep time        : {sleep_time} seconds")

    if expected_end_id is not None:
        print(
            "Expected ID range : "
            f"{expected_start_id}-{expected_end_id}"
        )
    else:
        print(
            "Expected ID range : "
            f"{expected_start_id}-end of dataset"
        )

    print("-" * 70)

    try:
        for chunk in pd.read_csv(
            dataset_path,
            chunksize=chunk_size,
        ):
            chunk_start = rows_seen
            chunk_end = chunk_start + len(chunk)

            rows_seen = chunk_end

            # Skip a complete chunk when it appears entirely before
            # the requested start row.
            if chunk_end <= start_row:
                continue

            # Calculate where the requested range begins inside
            # the current chunk.
            local_start = max(
                start_row - chunk_start,
                0,
            )

            selected_chunk = chunk.iloc[local_start:]

            # Keep only the number of records still required.
            if limit is not None:
                remaining = limit - total_sent

                if remaining <= 0:
                    break

                selected_chunk = selected_chunk.iloc[
                    :remaining
                ]

            records = selected_chunk.to_dict(
                orient="records"
            )

            for local_position, record in enumerate(
                records
            ):
                absolute_row_index = (
                    chunk_start
                    + local_start
                    + local_position
                )

                transaction = {
                    key: convert_value(value)
                    for key, value in record.items()
                }

                transaction["transaction_id"] = (
                    absolute_row_index + 1
                )

                transaction["stream_timestamp"] = (
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                )

                producer.send(
                    topic_name,
                    key=transaction["transaction_id"],
                    value=transaction,
                )

                total_sent += 1

                if (
                    total_sent == 1
                    or total_sent % 100 == 0
                ):
                    print(
                        f"Sent {total_sent} transactions "
                        f"| latest transaction_id="
                        f"{transaction['transaction_id']}"
                    )

                if sleep_time > 0:
                    time.sleep(sleep_time)

                if (
                    limit is not None
                    and total_sent >= limit
                ):
                    break

            if (
                limit is not None
                and total_sent >= limit
            ):
                break

        producer.flush()

        print("-" * 70)

        if total_sent == 0:
            print(
                "No transactions were sent."
            )
            print(
                "The selected start row may be beyond "
                "the end of the dataset."
            )
            return

        actual_start_id = start_row + 1
        actual_end_id = start_row + total_sent

        print(
            "Streaming completed. "
            f"Total transactions sent: {total_sent}"
        )

        print(
            "Transaction ID range: "
            f"{actual_start_id}-{actual_end_id}"
        )

        if (
            limit is not None
            and total_sent < limit
        ):
            print(
                "Warning: the dataset ended before "
                f"{limit} transactions were available."
            )

    finally:
        producer.close()


def parse_arguments() -> argparse.Namespace:
    """
    Parse and return command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Kafka producer for streaming "
            "PaySim transaction data"
        )
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
        help=(
            "Kafka bootstrap server for "
            "the local producer"
        ),
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50000,
        help="Number of rows to read at one time",
    )

    parser.add_argument(
        "--start-row",
        type=int,
        default=0,
        help=(
            "Zero-based dataset row from which "
            "streaming begins. Example: "
            "--start-row 2000 skips the first "
            "2000 rows"
        ),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help=(
            "Maximum number of transactions "
            "to stream"
        ),
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.05,
        help=(
            "Delay between messages to simulate "
            "real-time streaming"
        ),
    )

    args = parser.parse_args()

    try:
        validate_arguments(
            start_row=args.start_row,
            chunk_size=args.chunk_size,
            limit=args.limit,
            sleep_time=args.sleep,
        )
    except ValueError as error:
        parser.error(str(error))

    return args


def main() -> None:
    """
    Run the Kafka transaction producer.
    """

    args = parse_arguments()

    stream_transactions(
        file_path=args.file_path,
        topic_name=args.topic,
        bootstrap_server=args.bootstrap_server,
        chunk_size=args.chunk_size,
        start_row=args.start_row,
        limit=args.limit,
        sleep_time=args.sleep,
    )


if __name__ == "__main__":
    main()