"""Spark Structured Streaming fraud prediction, MongoDB and monitoring pipeline.

Pipeline stages:

1. Read PaySim transaction events from Kafka.
2. Parse Kafka JSON using an explicit Spark schema.
3. Process every Spark micro-batch with foreachBatch().
4. Reuse the existing Random Forest prediction helper.
5. Generate fraud predictions and probabilities.
6. Calculate batch-level validation and performance metrics.
7. Upsert all predictions into MongoDB prediction_history.
8. Upsert predicted fraud into MongoDB fraud_alerts.
9. Export operational metrics through Prometheus.

Run this file as a module from the repository root:

    python -m src.streaming.spark_fraud_pipeline --reset-checkpoint

Prometheus metrics endpoint:

    http://localhost:8000/metrics
"""

from __future__ import annotations

import argparse
import importlib
import shutil
import sys
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.database.connection import (
    close_mongo_client,
    save_prediction_batch,
    test_connection,
)
from src.monitoring.metrics import (
    record_batch_failure,
    record_completed_batch,
    record_empty_batch,
    start_metrics_server,
)


ROOT_DIR = Path(__file__).resolve().parents[2]

CHECKPOINT_DIR = (
    ROOT_DIR
    / "checkpoints"
    / "spark_fraud_pipeline"
)

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "transactions"

SPARK_KAFKA_PACKAGE = (
    "org.apache.spark:"
    "spark-sql-kafka-0-10_2.12:"
    "3.5.1"
)

MODEL_NAME = "RandomForestClassifier"

PROMETHEUS_HOST = "0.0.0.0"
PROMETHEUS_PORT = 8000


TRANSACTION_SCHEMA = StructType(
    [
        StructField(
            "transaction_id",
            LongType(),
            True,
        ),
        StructField(
            "step",
            IntegerType(),
            True,
        ),
        StructField(
            "type",
            StringType(),
            True,
        ),
        StructField(
            "amount",
            DoubleType(),
            True,
        ),
        StructField(
            "nameOrig",
            StringType(),
            True,
        ),
        StructField(
            "oldbalanceOrg",
            DoubleType(),
            True,
        ),
        StructField(
            "newbalanceOrig",
            DoubleType(),
            True,
        ),
        StructField(
            "nameDest",
            StringType(),
            True,
        ),
        StructField(
            "oldbalanceDest",
            DoubleType(),
            True,
        ),
        StructField(
            "newbalanceDest",
            DoubleType(),
            True,
        ),
        StructField(
            "isFraud",
            IntegerType(),
            True,
        ),
        StructField(
            "isFlaggedFraud",
            IntegerType(),
            True,
        ),
        StructField(
            "stream_timestamp",
            StringType(),
            True,
        ),
    ]
)


def parse_arguments() -> argparse.Namespace:
    """Read command-line options."""

    parser = argparse.ArgumentParser(
        description=(
            "Run the Spark fraud prediction, MongoDB persistence "
            "and Prometheus monitoring pipeline."
        )
    )

    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help=(
            "Delete the existing Spark checkpoint "
            "before starting."
        ),
    )

    return parser.parse_args()


def load_prediction_function() -> Callable[
    [dict[str, Any]],
    dict[str, Any],
]:
    """Load the existing reusable prediction helper."""

    module = importlib.import_module(
        "src.models.predict_fraud"
    )

    possible_function_names = (
        "predict_fraud",
        "predict_transaction",
        "predict_single_transaction",
        "make_prediction",
    )

    for function_name in possible_function_names:
        function = getattr(
            module,
            function_name,
            None,
        )

        if callable(function):
            print(
                "Prediction helper : "
                f"src.models.predict_fraud."
                f"{function_name}"
            )

            return function

    raise AttributeError(
        "No supported prediction function was found in "
        "src/models/predict_fraud.py. Expected one of: "
        + ", ".join(possible_function_names)
    )


PREDICT_TRANSACTION = load_prediction_function()


def prepare_checkpoint_directory(
    reset_checkpoint: bool,
) -> None:
    """Prepare the Spark checkpoint directory."""

    if reset_checkpoint and CHECKPOINT_DIR.exists():
        shutil.rmtree(CHECKPOINT_DIR)

        print(
            "Old checkpoint removed:",
            CHECKPOINT_DIR,
        )

    CHECKPOINT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        "Checkpoint directory:",
        CHECKPOINT_DIR,
    )


def create_spark_session() -> SparkSession:
    """Create the local Spark session."""

    spark = (
        SparkSession.builder
        .appName(
            "RealTimeFraudPredictionMonitoringPipeline"
        )
        .master("local[*]")
        .config(
            "spark.jars.packages",
            SPARK_KAFKA_PACKAGE,
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4",
        )
        .config(
            "spark.sql.adaptive.enabled",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def create_kafka_stream(
    spark: SparkSession,
) -> DataFrame:
    """Create the Kafka streaming DataFrame."""

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVER,
        )
        .option(
            "subscribe",
            KAFKA_TOPIC,
        )
        .option(
            "startingOffsets",
            "latest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )


def parse_transaction_stream(
    kafka_stream_df: DataFrame,
) -> DataFrame:
    """Parse Kafka JSON values into transaction columns."""

    return (
        kafka_stream_df
        .select(
            col("value")
            .cast("string")
            .alias("json_value")
        )
        .select(
            from_json(
                col("json_value"),
                TRANSACTION_SCHEMA,
            ).alias("transaction")
        )
        .select("transaction.*")
    )


def normalize_prediction_result(
    prediction_result: Any,
    transaction: dict[str, Any],
) -> dict[str, Any]:
    """Convert prediction output into one database-ready format."""

    if not isinstance(prediction_result, dict):
        raise TypeError(
            "The prediction helper must return a dictionary."
        )

    prediction = prediction_result.get(
        "prediction"
    )

    fraud_probability = prediction_result.get(
        "fraud_probability",
        prediction_result.get("probability"),
    )

    threshold = prediction_result.get(
        "threshold",
        0.5,
    )

    model_used = prediction_result.get(
        "model_used",
        MODEL_NAME,
    )

    if prediction is None:
        raise ValueError(
            "Prediction output does not contain prediction."
        )

    if fraud_probability is None:
        raise ValueError(
            "Prediction output does not contain "
            "fraud_probability."
        )

    return {
        "transaction_id": int(
            transaction["transaction_id"]
        ),
        "step": int(
            transaction.get("step") or 0
        ),
        "type": transaction.get("type"),
        "amount": float(
            transaction.get("amount") or 0.0
        ),
        "nameOrig": transaction.get(
            "nameOrig"
        ),
        "oldbalanceOrg": float(
            transaction.get("oldbalanceOrg") or 0.0
        ),
        "newbalanceOrig": float(
            transaction.get("newbalanceOrig") or 0.0
        ),
        "nameDest": transaction.get(
            "nameDest"
        ),
        "oldbalanceDest": float(
            transaction.get("oldbalanceDest") or 0.0
        ),
        "newbalanceDest": float(
            transaction.get("newbalanceDest") or 0.0
        ),
        "actual_isFraud": int(
            transaction.get("isFraud") or 0
        ),
        "isFlaggedFraud": int(
            transaction.get("isFlaggedFraud") or 0
        ),
        "prediction": int(prediction),
        "fraud_probability": float(
            fraud_probability
        ),
        "threshold": float(threshold),
        "model_used": str(model_used),
        "stream_timestamp": transaction.get(
            "stream_timestamp"
        ),
    }


def print_prediction_table(
    prediction_records: list[dict[str, Any]],
) -> None:
    """Display prediction results in the terminal."""

    if not prediction_records:
        print("\nNo successful predictions to display.")
        return

    header = (
        f"{'ID':<6}"
        f"{'TYPE':<12}"
        f"{'AMOUNT':>14}"
        f"{'ACTUAL':>10}"
        f"{'PREDICTED':>12}"
        f"{'PROBABILITY':>15}"
    )

    print("\n" + header)
    print("-" * len(header))

    for record in prediction_records:
        print(
            f"{record['transaction_id']:<6}"
            f"{str(record['type']):<12}"
            f"{record['amount']:>14.2f}"
            f"{record['actual_isFraud']:>10}"
            f"{record['prediction']:>12}"
            f"{record['fraud_probability']:>15.6f}"
        )


def calculate_prediction_metrics(
    prediction_records: list[dict[str, Any]],
) -> dict[str, int]:
    """Calculate confusion-matrix values."""

    true_positive = 0
    true_negative = 0
    false_positive = 0
    false_negative = 0

    for record in prediction_records:
        actual = record["actual_isFraud"]
        predicted = record["prediction"]

        if actual == 1 and predicted == 1:
            true_positive += 1

        elif actual == 0 and predicted == 0:
            true_negative += 1

        elif actual == 0 and predicted == 1:
            false_positive += 1

        elif actual == 1 and predicted == 0:
            false_negative += 1

    return {
        "true_positive": true_positive,
        "true_negative": true_negative,
        "false_positive": false_positive,
        "false_negative": false_negative,
    }


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """Predict, persist and monitor one Spark micro-batch."""

    start_time = perf_counter()
    cached_batch_df = batch_df.cache()

    try:
        record_count = cached_batch_df.count()

        print("\n" + "=" * 90)
        print(
            f"SPARK BATCH ID          : {batch_id}"
        )
        print(
            f"RECORDS RECEIVED        : {record_count}"
        )

        if record_count == 0:
            record_empty_batch(
                batch_id=batch_id
            )

            print(
                "BATCH STATUS            : Empty batch"
            )
            print("=" * 90)
            return

        null_transaction_count = (
            cached_batch_df
            .filter(
                col("transaction_id").isNull()
            )
            .count()
        )

        print(
            "NULL TRANSACTION IDs    :",
            null_transaction_count,
        )

        if null_transaction_count > 0:
            raise ValueError(
                "The batch contains null transaction IDs."
            )

        transaction_rows = (
            cached_batch_df
            .orderBy("transaction_id")
            .collect()
        )

        prediction_records: list[
            dict[str, Any]
        ] = []

        failed_records = 0

        for row in transaction_rows:
            transaction = row.asDict(
                recursive=True
            )

            try:
                prediction_result = (
                    PREDICT_TRANSACTION(
                        transaction
                    )
                )

                normalized_result = (
                    normalize_prediction_result(
                        prediction_result,
                        transaction,
                    )
                )

                prediction_records.append(
                    normalized_result
                )

            except Exception as prediction_error:
                failed_records += 1

                print(
                    "Prediction failed for "
                    f"transaction_id="
                    f"{transaction.get('transaction_id')}: "
                    f"{prediction_error}",
                    file=sys.stderr,
                )

        successful_predictions = len(
            prediction_records
        )

        predicted_fraud_count = sum(
            record["prediction"] == 1
            for record in prediction_records
        )

        actual_fraud_count = sum(
            record["actual_isFraud"] == 1
            for record in prediction_records
        )

        prediction_metrics = (
            calculate_prediction_metrics(
                prediction_records
            )
        )

        print_prediction_table(
            prediction_records
        )

        mongo_summary = save_prediction_batch(
            prediction_records=prediction_records,
            spark_batch_id=batch_id,
        )

        processing_seconds = (
            perf_counter() - start_time
        )

        throughput = (
            successful_predictions
            / processing_seconds
            if processing_seconds > 0
            else 0.0
        )

        record_completed_batch(
            batch_id=batch_id,
            record_count=record_count,
            successful_predictions=successful_predictions,
            failed_predictions=failed_records,
            predicted_fraud_count=predicted_fraud_count,
            processing_seconds=processing_seconds,
            throughput=throughput,
            prediction_metrics=prediction_metrics,
            mongo_summary=mongo_summary,
        )

        print("\n" + "-" * 90)
        print(
            "SUCCESSFUL PREDICTIONS  :",
            successful_predictions,
        )
        print(
            "FAILED PREDICTIONS      :",
            failed_records,
        )
        print(
            "ACTUAL FRAUD LABELS     :",
            actual_fraud_count,
        )
        print(
            "PREDICTED FRAUD ALERTS  :",
            predicted_fraud_count,
        )
        print(
            "TRUE POSITIVES          :",
            prediction_metrics["true_positive"],
        )
        print(
            "TRUE NEGATIVES          :",
            prediction_metrics["true_negative"],
        )
        print(
            "FALSE POSITIVES         :",
            prediction_metrics["false_positive"],
        )
        print(
            "FALSE NEGATIVES         :",
            prediction_metrics["false_negative"],
        )

        print("\nMongoDB persistence")
        print(
            "PREDICTION OPERATIONS   :",
            mongo_summary[
                "prediction_operations"
            ],
        )
        print(
            "PREDICTION INSERTS      :",
            mongo_summary[
                "prediction_upserts"
            ],
        )
        print(
            "PREDICTION MATCHES      :",
            mongo_summary[
                "prediction_matches"
            ],
        )
        print(
            "FRAUD ALERT OPERATIONS  :",
            mongo_summary[
                "alert_operations"
            ],
        )
        print(
            "FRAUD ALERT INSERTS     :",
            mongo_summary[
                "alert_upserts"
            ],
        )
        print(
            "FRAUD ALERT MATCHES     :",
            mongo_summary[
                "alert_matches"
            ],
        )

        print("\nPrometheus monitoring")
        print(
            "METRICS RECORDED        : Yes"
        )
        print(
            "METRICS ENDPOINT        : "
            f"http://localhost:"
            f"{PROMETHEUS_PORT}/metrics"
        )

        print(
            "PROCESSING TIME         : "
            f"{processing_seconds:.4f} seconds"
        )
        print(
            "PREDICTION THROUGHPUT   : "
            f"{throughput:.2f} records/second"
        )
        print(
            "BATCH STATUS            : Completed"
        )
        print("=" * 90)

        if successful_predictions != record_count:
            print(
                "WARNING: Not every record received "
                "a successful prediction.",
                file=sys.stderr,
            )

    except Exception as error:
        record_batch_failure()

        print(
            f"Batch {batch_id} failed: {error}",
            file=sys.stderr,
        )

        raise

    finally:
        cached_batch_df.unpersist()


def stop_streaming_query(
    query: Any,
) -> None:
    """Stop the streaming query without masking the original error."""

    if query is None:
        return

    try:
        if query.isActive:
            query.stop()

    except Exception as error:
        print(
            "Streaming query cleanup warning:",
            error,
            file=sys.stderr,
        )


def stop_spark_session(
    spark: SparkSession | None,
) -> None:
    """Stop Spark without masking an earlier JVM failure."""

    if spark is None:
        return

    try:
        spark.stop()

    except Exception as error:
        print(
            "Spark cleanup warning:",
            error,
            file=sys.stderr,
        )


def main() -> None:
    """Start the complete real-time fraud pipeline."""

    args = parse_arguments()

    prepare_checkpoint_directory(
        reset_checkpoint=args.reset_checkpoint
    )

    spark: SparkSession | None = None
    query = None

    try:
        test_connection()

        start_metrics_server(
            host=PROMETHEUS_HOST,
            port=PROMETHEUS_PORT,
        )

        spark = create_spark_session()

        kafka_stream_df = create_kafka_stream(
            spark
        )

        transaction_stream_df = (
            parse_transaction_stream(
                kafka_stream_df
            )
        )

        query = (
            transaction_stream_df
            .writeStream
            .foreachBatch(process_batch)
            .option(
                "checkpointLocation",
                str(CHECKPOINT_DIR),
            )
            .start()
        )

        print(
            "\nSpark fraud prediction pipeline started."
        )
        print(
            "Kafka server       :",
            KAFKA_BOOTSTRAP_SERVER,
        )
        print(
            "Kafka topic        :",
            KAFKA_TOPIC,
        )
        print(
            "Processing mode    : foreachBatch"
        )
        print(
            "Prediction model   :",
            MODEL_NAME,
        )
        print(
            "MongoDB storage    : Enabled"
        )
        print(
            "Prediction history : "
            "fraud_detection_db.prediction_history"
        )
        print(
            "Fraud alerts       : "
            "fraud_detection_db.fraud_alerts"
        )
        print(
            "Prometheus metrics : "
            f"http://localhost:"
            f"{PROMETHEUS_PORT}/metrics"
        )
        print(
            "Waiting for transactions..."
        )
        print(
            "Press Ctrl+C to stop.\n"
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        print(
            "\nStopping Spark fraud prediction pipeline..."
        )

    except Exception as error:
        print(
            f"\nSpark pipeline failed: {error}",
            file=sys.stderr,
        )

        raise

    finally:
        stop_streaming_query(query)
        stop_spark_session(spark)
        close_mongo_client()

        print("Spark session stopped.")


if __name__ == "__main__":
    main()