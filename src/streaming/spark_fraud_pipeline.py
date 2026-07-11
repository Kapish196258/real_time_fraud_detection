"""Spark Structured Streaming fraud pipeline.
This first version:
1. Reads transaction JSON messages from Kafka.
2. Parses them using a fixed Spark schema.
3. Processes each micro-batch using foreachBatch().
4. Displays batch counts, selected records, processing time and throughput.

Machine-learning prediction and MongoDB storage will be added in the next stage."""

from __future__ import annotations
import argparse
import shutil
import sys
from pathlib import Path
from time import perf_counter

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

# Project paths and streaming configuration
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

# Kafka transaction schema
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", LongType(), True),
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True),
        StructField("stream_timestamp", StringType(), True),
    ]
)

# Utility functions
def parse_arguments() -> argparse.Namespace:
    """Read command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Run the Spark foreachBatch fraud pipeline."
    )

    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help=(
            "Delete the existing Spark checkpoint before starting. "
            "Use this only for a fresh controlled test."
        ),
    )

    return parser.parse_args()


def prepare_checkpoint_directory(reset_checkpoint: bool) -> None:
    """Create the checkpoint directory and optionally reset old state."""

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
    """Create and configure the Spark session."""

    spark = (
        SparkSession.builder
        .appName("RealTimeFraudSparkPipeline")
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
    """Read transaction messages continuously from Kafka."""

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
    """Convert Kafka values from JSON into structured columns."""

    parsed_df = (
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

    return parsed_df


# Micro-batch processing
def process_batch(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """Process one Spark micro-batch."""

    start_time = perf_counter()

    cached_batch_df = batch_df.cache()

    try:
        record_count = cached_batch_df.count()

        print("\n" + "=" * 80)
        print(f"SPARK BATCH ID       : {batch_id}")
        print(f"RECORDS RECEIVED     : {record_count}")

        if record_count == 0:
            print("BATCH STATUS         : Empty batch")
            print("=" * 80)
            return

        null_transaction_count = (
            cached_batch_df
            .filter(
                col("transaction_id").isNull()
            )
            .count()
        )

        fraud_label_count = (
            cached_batch_df
            .filter(
                col("isFraud") == 1
            )
            .count()
        )

        print(
            "NULL TRANSACTION IDs :",
            null_transaction_count,
        )
        print(
            "FRAUD LABELS FOUND   :",
            fraud_label_count,
        )

        cached_batch_df.select(
            "transaction_id",
            "step",
            "type",
            "amount",
            "nameOrig",
            "nameDest",
            "isFraud",
            "stream_timestamp",
        ).orderBy(
            "transaction_id"
        ).show(
            n=20,
            truncate=False,
        )

        processing_seconds = (
            perf_counter() - start_time
        )

        throughput = (
            record_count / processing_seconds
            if processing_seconds > 0
            else 0.0
        )

        print(
            "PROCESSING TIME       : "
            f"{processing_seconds:.4f} seconds"
        )

        print(
            "BATCH THROUGHPUT      : "
            f"{throughput:.2f} records/second"
        )

        print("BATCH STATUS         : Completed")
        print("=" * 80)

    except Exception as error:
        print(
            f"Batch {batch_id} failed: {error}",
            file=sys.stderr,
        )
        raise

    finally:
        cached_batch_df.unpersist()


# Main streaming application
def main() -> None:
    """Start the Spark fraud-processing pipeline."""

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

        print("\nSpark fraud pipeline started.")
        print(
            "Kafka server     :",
            KAFKA_BOOTSTRAP_SERVER,
        )
        print(
            "Kafka topic      :",
            KAFKA_TOPIC,
        )
        print(
            "Processing mode  : foreachBatch"
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
            .foreachBatch(process_batch)
            .option(
                "checkpointLocation",
                str(CHECKPOINT_DIR),
            )
            .start()
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        print(
            "\nStopping Spark fraud pipeline..."
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

        print("Spark session stopped.")


if __name__ == "__main__":
    main()