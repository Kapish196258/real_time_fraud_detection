"""Prometheus metrics for the Spark fraud-detection pipeline.

This module exposes operational metrics through an HTTP endpoint.

Default endpoint:

    http://localhost:8000/metrics

Prometheus will later scrape this endpoint and Grafana will visualize
the collected time-series data.
"""

from __future__ import annotations

import threading
from typing import Any

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)


DEFAULT_METRICS_HOST = "0.0.0.0"
DEFAULT_METRICS_PORT = 8000

_metrics_server_started = False
_metrics_server_lock = threading.Lock()


TRANSACTIONS_PROCESSED = Counter(
    "fraud_transactions_processed",
    "Total number of transactions successfully scored.",
)

FRAUD_PREDICTIONS = Counter(
    "fraud_predictions",
    "Total number of transactions predicted as fraud.",
)

PREDICTION_FAILURES = Counter(
    "fraud_prediction_failures",
    "Total number of failed model predictions.",
)

SPARK_BATCHES_PROCESSED = Counter(
    "fraud_spark_batches_processed",
    "Total number of completed non-empty Spark batches.",
)

SPARK_EMPTY_BATCHES = Counter(
    "fraud_spark_empty_batches",
    "Total number of empty Spark batches.",
)

SPARK_BATCH_FAILURES = Counter(
    "fraud_spark_batch_failures",
    "Total number of Spark micro-batches that failed.",
)

TRUE_POSITIVES = Counter(
    "fraud_true_positives",
    "Total fraud transactions correctly predicted as fraud.",
)

TRUE_NEGATIVES = Counter(
    "fraud_true_negatives",
    "Total normal transactions correctly predicted as normal.",
)

FALSE_POSITIVES = Counter(
    "fraud_false_positives",
    "Total normal transactions incorrectly predicted as fraud.",
)

FALSE_NEGATIVES = Counter(
    "fraud_false_negatives",
    "Total fraud transactions incorrectly predicted as normal.",
)

MONGODB_PREDICTION_OPERATIONS = Counter(
    "fraud_mongodb_prediction_operations",
    "Total MongoDB prediction-history write operations.",
)

MONGODB_PREDICTION_INSERTS = Counter(
    "fraud_mongodb_prediction_inserts",
    "Total prediction-history documents inserted through upsert.",
)

MONGODB_PREDICTION_MATCHES = Counter(
    "fraud_mongodb_prediction_matches",
    "Total existing prediction-history documents matched by upsert.",
)

MONGODB_ALERT_OPERATIONS = Counter(
    "fraud_mongodb_alert_operations",
    "Total MongoDB fraud-alert write operations.",
)

MONGODB_ALERT_INSERTS = Counter(
    "fraud_mongodb_alert_inserts",
    "Total fraud-alert documents inserted through upsert.",
)

MONGODB_ALERT_MATCHES = Counter(
    "fraud_mongodb_alert_matches",
    "Total existing fraud-alert documents matched by upsert.",
)

LATEST_BATCH_ID = Gauge(
    "fraud_spark_latest_batch_id",
    "Most recently completed Spark batch ID.",
)

LATEST_BATCH_SIZE = Gauge(
    "fraud_spark_latest_batch_size",
    "Number of records in the latest completed Spark batch.",
)

LATEST_BATCH_PROCESSING_SECONDS = Gauge(
    "fraud_spark_latest_batch_processing_seconds",
    "Processing duration of the latest completed Spark batch.",
)

LATEST_PREDICTION_THROUGHPUT = Gauge(
    "fraud_spark_latest_prediction_throughput",
    "Prediction throughput of the latest completed Spark batch.",
)

LATEST_FRAUD_COUNT = Gauge(
    "fraud_spark_latest_fraud_count",
    "Predicted fraud count in the latest completed Spark batch.",
)

BATCH_PROCESSING_SECONDS = Histogram(
    "fraud_spark_batch_processing_seconds",
    "Distribution of Spark batch processing durations.",
    buckets=(
        0.1,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
        60.0,
        120.0,
        300.0,
    ),
)


def start_metrics_server(
    host: str = DEFAULT_METRICS_HOST,
    port: int = DEFAULT_METRICS_PORT,
) -> None:
    """Start the Prometheus HTTP metrics server once."""

    global _metrics_server_started

    with _metrics_server_lock:
        if _metrics_server_started:
            return

        start_http_server(
            port=port,
            addr=host,
        )

        _metrics_server_started = True

        print(
            "Prometheus metrics : "
            f"http://localhost:{port}/metrics"
        )


def record_empty_batch(
    batch_id: int,
) -> None:
    """Record one empty Spark micro-batch."""

    SPARK_EMPTY_BATCHES.inc()
    LATEST_BATCH_ID.set(batch_id)
    LATEST_BATCH_SIZE.set(0)
    LATEST_FRAUD_COUNT.set(0)


def record_completed_batch(
    *,
    batch_id: int,
    record_count: int,
    successful_predictions: int,
    failed_predictions: int,
    predicted_fraud_count: int,
    processing_seconds: float,
    throughput: float,
    prediction_metrics: dict[str, int],
    mongo_summary: dict[str, Any],
) -> None:
    """Record metrics for one completed non-empty Spark batch."""

    SPARK_BATCHES_PROCESSED.inc()

    TRANSACTIONS_PROCESSED.inc(
        successful_predictions
    )

    FRAUD_PREDICTIONS.inc(
        predicted_fraud_count
    )

    PREDICTION_FAILURES.inc(
        failed_predictions
    )

    TRUE_POSITIVES.inc(
        prediction_metrics["true_positive"]
    )

    TRUE_NEGATIVES.inc(
        prediction_metrics["true_negative"]
    )

    FALSE_POSITIVES.inc(
        prediction_metrics["false_positive"]
    )

    FALSE_NEGATIVES.inc(
        prediction_metrics["false_negative"]
    )

    MONGODB_PREDICTION_OPERATIONS.inc(
        mongo_summary["prediction_operations"]
    )

    MONGODB_PREDICTION_INSERTS.inc(
        mongo_summary["prediction_upserts"]
    )

    MONGODB_PREDICTION_MATCHES.inc(
        mongo_summary["prediction_matches"]
    )

    MONGODB_ALERT_OPERATIONS.inc(
        mongo_summary["alert_operations"]
    )

    MONGODB_ALERT_INSERTS.inc(
        mongo_summary["alert_upserts"]
    )

    MONGODB_ALERT_MATCHES.inc(
        mongo_summary["alert_matches"]
    )

    LATEST_BATCH_ID.set(batch_id)
    LATEST_BATCH_SIZE.set(record_count)

    LATEST_BATCH_PROCESSING_SECONDS.set(
        processing_seconds
    )

    LATEST_PREDICTION_THROUGHPUT.set(
        throughput
    )

    LATEST_FRAUD_COUNT.set(
        predicted_fraud_count
    )

    BATCH_PROCESSING_SECONDS.observe(
        processing_seconds
    )


def record_batch_failure() -> None:
    """Record one failed Spark micro-batch."""

    SPARK_BATCH_FAILURES.inc()