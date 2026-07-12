"""Spark Structured Streaming fraud prediction pipeline.

Current stage:
1. Read PaySim transactions from Kafka.
2. Parse Kafka JSON into structured Spark columns.
3. Process every micro-batch using foreachBatch().
4. Reuse the existing predict_fraud.py helper.
5. Generate model prediction and fraud probability.
6. Compare model predictions with the original PaySim fraud label.
7. Display batch-level prediction and performance metrics.

MongoDB storage will be added only after this stage is validated.
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


# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Kafka transaction schema
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Command-line arguments
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    """Read command-line options."""

    parser = argparse.ArgumentParser(
        description=(
            "Run Spark Structured Streaming "
            "fraud prediction pipeline."
        )
    )

    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help=(
            "Delete the current Spark checkpoint "
            "before starting a clean test."
        ),
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Existing prediction helper integration
# ---------------------------------------------------------------------------

def load_prediction_function() -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Load the prediction function from src.models.predict_fraud.

    The project already contains reusable prediction logic in
    src/models/predict_fraud.py. Reusing that helper prevents the Spark
    pipeline from creating a second and possibly inconsistent feature
    engineering implementation.
    """

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
                f"src.models.predict_fraud.{function_name}"
            )

            return function

    raise AttributeError(
        "No supported prediction function was found inside "
        "src/models/predict_fraud.py. Expected one of: "
        + ", ".join(possible_function_names)
    )


PREDICT_TRANSACTION = load_prediction_function()


# ---------------------------------------------------------------------------
# Checkpoint handling
# ---------------------------------------------------------------------------

def prepare_checkpoint_directory(
    reset_checkpoint: bool,
) -> None:
    """Prepare Spark checkpoint storage."""

    if reset_checkpoint and CHECKPOINT_DIR.exists():
        shutil.rmtree(
            CHECKPOINT_DIR
        )

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


# ---------------------------------------------------------------------------
# Spark session and Kafka stream
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """Create the local Spark session."""

    spark = (
        SparkSession.builder
        .appName(
            "RealTimeFraudPredictionPipeline"
        )
        .master(
            "local[*]"
        )
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

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


def create_kafka_stream(
    spark: SparkSession,
) -> DataFrame:
    """Create the Kafka streaming DataFrame."""

    return (
        spark.readStream
        .format(
            "kafka"
        )
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
        .select(
            "transaction.*"
        )
    )


# ---------------------------------------------------------------------------
# Prediction result normalization
# ---------------------------------------------------------------------------

def normalize_prediction_result(
    prediction_result: Any,
    transaction: dict[str, Any],
) -> dict[str, Any]:
    """Convert prediction-helper output to one consistent format."""

    if not isinstance(
        prediction_result,
        dict,
    ):
        raise TypeError(
            "The prediction helper must return a dictionary."
        )

    prediction = prediction_result.get(
        "prediction"
    )

    fraud_probability = prediction_result.get(
        "fraud_probability",
        prediction_result.get(
            "probability"
        ),
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
            "Prediction result does not contain "
            "the 'prediction' field."
        )

    if fraud_probability is None:
        raise ValueError(
            "Prediction result does not contain "
            "'fraud_probability'."
        )

    return {
        "transaction_id": int(
            transaction["transaction_id"]
        ),
        "type": transaction["type"],
        "amount": float(
            transaction["amount"]
        ),
        "actual_isFraud": int(
            transaction.get(
                "isFraud",
                0,
            )
        ),
        "prediction": int(
            prediction
        ),
        "fraud_probability": float(
            fraud_probability
        ),
        "threshold": float(
            threshold
        ),
        "model_used": str(
            model_used
        ),
        "stream_timestamp": transaction.get(
            "stream_timestamp"
        ),
    }


# ---------------------------------------------------------------------------
# Console table
# ---------------------------------------------------------------------------

def print_prediction_table(
    prediction_records: list[dict[str, Any]],
) -> None:
    """Display prediction results in a readable table."""

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
            f"{record['type']:<12}"
            f"{record['amount']:>14.2f}"
            f"{record['actual_isFraud']:>10}"
            f"{record['prediction']:>12}"
            f"{record['fraud_probability']:>15.6f}"
        )


# ---------------------------------------------------------------------------
# Batch metrics
# ---------------------------------------------------------------------------

def calculate_prediction_metrics(
    prediction_records: list[dict[str, Any]],
) -> dict[str, int]:
    """Calculate validation metrics for one micro-batch."""

    true_positive = 0
    true_negative = 0
    false_positive = 0
    false_negative = 0

    for record in prediction_records:
        actual = record[
            "actual_isFraud"
        ]

        predicted = record[
            "prediction"
        ]

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


# ---------------------------------------------------------------------------
# Spark foreachBatch processing
# ---------------------------------------------------------------------------

def process_batch(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """Run model prediction for one Spark micro-batch."""

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
            .orderBy(
                "transaction_id"
            )
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

        metrics = calculate_prediction_metrics(
            prediction_records
        )

        print_prediction_table(
            prediction_records
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
            metrics["true_positive"],
        )
        print(
            "TRUE NEGATIVES          :",
            metrics["true_negative"],
        )
        print(
            "FALSE POSITIVES         :",
            metrics["false_positive"],
        )
        print(
            "FALSE NEGATIVES         :",
            metrics["false_negative"],
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
        print(
            f"Batch {batch_id} failed: {error}",
            file=sys.stderr,
        )
        raise

    finally:
        cached_batch_df.unpersist()


# ---------------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------------

def main() -> None:
    """Start the Spark fraud prediction stream."""

    args = parse_arguments()

    prepare_checkpoint_directory(
        reset_checkpoint=args.reset_checkpoint
    )

    spark: SparkSession | None = None
    query = None

    try:
        spark = create_spark_session()

        kafka_stream_df = create_kafka_stream(
            spark
        )

        transaction_stream_df = (
            parse_transaction_stream(
                kafka_stream_df
            )
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
            "MongoDB storage    : Not enabled in this stage"
        )
        print(
            "Waiting for transactions..."
        )
        print(
            "Press Ctrl+C to stop.\n"
        )

        query = (
            transaction_stream_df
            .writeStream
            .foreachBatch(
                process_batch
            )
            .option(
                "checkpointLocation",
                str(CHECKPOINT_DIR),
            )
            .start()
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
        if query is not None and query.isActive:
            query.stop()

        if spark is not None:
            spark.stop()

        print(
            "Spark session stopped."
        )


if __name__ == "__main__":
    main()