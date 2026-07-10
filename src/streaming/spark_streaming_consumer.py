import os
import shutil
from pathlib import Path


HADOOP_HOME = "D:\\hadoop"

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = f"{HADOOP_HOME}\\bin;" + os.environ.get("PATH", "")
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
CHECKPOINT_DIR = ROOT_DIR / "checkpoints" / "spark_streaming_consumer"


def clean_checkpoint_directory() -> None:
    """
    Removes old Spark checkpoint data before starting a fresh local test run.
    This avoids old failed streaming metadata causing repeated errors.
    """
    if CHECKPOINT_DIR.exists():
        shutil.rmtree(CHECKPOINT_DIR)

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


def create_spark_session() -> SparkSession:
    """
    Creates Spark session with Kafka package support.
    This allows Spark Structured Streaming to read data from Kafka.
    """
    spark = (
        SparkSession.builder
        .appName("RealTimeFraudDetectionSparkConsumer")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_transaction_schema() -> StructType:
    """
    Defines schema for PaySim transaction JSON messages coming from Kafka.
    """
    return StructType(
        [
            StructField("transaction_id", IntegerType(), True),
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


def main() -> None:
    clean_checkpoint_directory()

    spark = create_spark_session()
    transaction_schema = get_transaction_schema()

    print("=" * 80)
    print("Spark Structured Streaming Consumer")
    print("=" * 80)
    print("Kafka bootstrap server : localhost:9092")
    print("Kafka topic            : transactions")
    print(f"Checkpoint directory   : {CHECKPOINT_DIR}")
    print("Waiting for Kafka messages...")
    print("Press Ctrl+C to stop.")
    print("=" * 80)

    raw_kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream_df = (
        raw_kafka_stream_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), transaction_schema).alias("data"))
        .select("data.*")
    )

    selected_stream_df = parsed_stream_df.select(
        "transaction_id",
        "step",
        "type",
        "amount",
        "nameOrig",
        "nameDest",
        "isFraud",
        "stream_timestamp",
    )

    query = (
        selected_stream_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "20")
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nSpark streaming consumer stopped manually.")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()