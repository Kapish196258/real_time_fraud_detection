# Grafana Pipeline Performance Dashboard Validation Report

## Purpose

This report documents the functional validation of the **Fraud Pipeline Performance** Grafana dashboard developed for the Real-Time Fraud Detection project.

The dashboard is designed to monitor the operational performance of the real-time fraud detection pipeline, including:

- Spark micro-batch processing;
- prediction throughput;
- batch-processing duration;
- Spark and prediction failures;
- MongoDB prediction operations;
- MongoDB fraud-alert operations.

The validation was performed using a live producer run containing 2,000 PaySim transactions.

---

## Dashboard Information

| Property | Value |
|---|---|
| Dashboard title | Fraud Pipeline Performance |
| Dashboard UID | `fraud-performance` |
| Dashboard file | `monitoring/grafana/dashboards/fraud-performance.json` |
| Datasource type | Prometheus |
| Datasource UID | `fraud-prometheus` |
| Refresh interval | 5 seconds |
| Default time range | Last 30 minutes |
| Total panels | 10 |

---

## Dashboard Panels

The dashboard contains the following panels:

| Panel ID | Panel title | Visualization |
|---:|---|---|
| 1 | Latest Batch Size | Stat |
| 2 | Latest Batch Duration | Stat |
| 3 | Latest Prediction Throughput | Stat |
| 4 | Completed Spark Batches | Stat |
| 5 | Spark Batch Failures | Stat |
| 6 | Prediction Failures | Stat |
| 7 | Batch Processing Duration Over Time | Time series |
| 8 | Prediction Throughput Over Time | Time series |
| 9 | MongoDB Prediction Operations | Time series |
| 10 | MongoDB Fraud Alert Operations | Time series |

The dashboard uses Prometheus metrics exported by the Spark fraud prediction pipeline.

---

## Dashboard JSON Validation

The dashboard JSON file was validated using PowerShell:

```powershell
Get-Content `
    .\monitoring\grafana\dashboards\fraud-performance.json `
    -Raw |
ConvertFrom-Json |
Out-Null
```

The command completed without an error, confirming that the dashboard file contains valid JSON.

The dashboard configuration was also reviewed and confirmed to contain:

- dashboard title `Fraud Pipeline Performance`;
- dashboard UID `fraud-performance`;
- Prometheus datasource UID `fraud-prometheus`;
- refresh interval of `5s`;
- time range from `now-30m` to `now`;
- six stat panels;
- four time-series panels;
- ten panels in total.

---

## Test Environment

The validation used the following project components:

- PaySim transaction dataset;
- Python Kafka producer;
- Apache Kafka;
- Spark Structured Streaming;
- Random Forest fraud prediction model;
- MongoDB;
- Prometheus;
- Grafana;
- Docker Compose.

The complete Docker environment was started with:

```powershell
docker compose -f .\docker\docker-compose.yml up -d
```

The running containers were checked using:

```powershell
docker ps
```

This ensured that Kafka, MongoDB, Prometheus, Grafana, Kafka UI, and Mongo Express were available before starting the live pipeline test.

---

## Grafana Service Validation

Grafana was restarted after the new dashboard was merged:

```powershell
docker compose `
    -f .\docker\docker-compose.yml `
    up -d `
    --force-recreate grafana
```

Grafana health was checked using:

```powershell
Start-Sleep -Seconds 10

Invoke-RestMethod http://localhost:3000/api/health
```

The health check returned:

```text
database : ok
```

This confirmed that the Grafana service was available and connected to its internal database.

---

## Spark Fraud Pipeline Startup

The Spark fraud prediction pipeline was started from the project root using module execution:

```powershell
python -m src.streaming.spark_fraud_pipeline --reset-checkpoint
```

Module execution was required because the pipeline imports project modules using paths such as:

```python
from src.database.connection import ...
```

The Spark pipeline started successfully and displayed the following configuration:

```text
Spark fraud prediction pipeline started.
Kafka server       : localhost:9092
Kafka topic        : transactions
Processing mode    : foreachBatch
Prediction model   : RandomForestClassifier
MongoDB storage    : Enabled
Prediction history : fraud_detection_db.prediction_history
Fraud alerts       : fraud_detection_db.fraud_alerts
Prometheus metrics : http://localhost:8000/metrics
Waiting for transactions...
Press Ctrl+C to stop.
```

This confirmed that:

- Spark connected to Kafka;
- the Random Forest prediction helper loaded;
- MongoDB storage was enabled;
- Prometheus metrics were exposed on port `8000`;
- the streaming query was waiting for transactions.

---

## Prometheus Metrics Server Validation

The Prometheus metrics endpoint was available at:

```text
http://localhost:8000/metrics
```

Port `8000` was checked using:

```powershell
Test-NetConnection localhost -Port 8000
```

The result included:

```text
ComputerName     : localhost
RemoteAddress    : 127.0.0.1
RemotePort       : 8000
TcpTestSucceeded : True
```

The warning for the IPv6 address `::1` was not a failure because the connection succeeded through the IPv4 address `127.0.0.1`.

The exported metrics were inspected using:

```powershell
Invoke-WebRequest `
    http://localhost:8000/metrics `
    -UseBasicParsing |
Select-Object -ExpandProperty Content |
Select-String `
    "fraud_spark_latest_batch_size|fraud_spark_latest_batch_processing_seconds|fraud_spark_latest_prediction_throughput|fraud_spark_batches_processed_total|fraud_spark_batch_failures_total|fraud_prediction_failures_total|fraud_mongodb_prediction|fraud_mongodb_alert"
```

The dashboard uses the following Prometheus metrics:

```text
fraud_spark_latest_batch_size
fraud_spark_latest_batch_processing_seconds
fraud_spark_latest_prediction_throughput
fraud_spark_batches_processed_total
fraud_spark_batch_failures_total
fraud_prediction_failures_total
fraud_mongodb_prediction_operations_total
fraud_mongodb_prediction_inserts_total
fraud_mongodb_prediction_matches_total
fraud_mongodb_alert_operations_total
fraud_mongodb_alert_inserts_total
fraud_mongodb_alert_matches_total
```

These metrics provide the data required by all ten performance-dashboard panels.

---

## Producer Load-Test Configuration

The producer was executed using a unique and non-overlapping transaction range:

```powershell
python .\src\streaming\producer.py `
    --start-row 6000 `
    --limit 2000 `
    --sleep 0.01
```

The producer configuration was:

| Setting | Value |
|---|---:|
| Dataset start row | 6000 |
| Transaction limit | 2000 |
| Delay between messages | 0.01 seconds |
| First transaction ID | 6001 |
| Last transaction ID | 8000 |

The producer displayed:

```text
Starting transaction streaming...
Dataset path      : data\raw\paysim_transactions.csv
Kafka topic       : transactions
Bootstrap server  : localhost:9092
Chunk size        : 50000
Start row         : 6000
Limit             : 2000
Sleep time        : 0.01 seconds
Expected ID range : 6001-8000
```

The producer completed successfully:

```text
Sent 2000 transactions | latest transaction_id=8000
----------------------------------------------------------------------
Streaming completed. Total transactions sent: 2000
Transaction ID range: 6001-8000
```

This confirmed that:

- exactly 2,000 transactions were sent;
- the producer started from dataset row 6000;
- transaction IDs were generated from 6001 through 8000;
- the transaction range did not overlap with earlier test ranges.

---

## Spark Micro-Batch Results

Spark initially created an empty batch before receiving producer messages:

```text
SPARK BATCH ID          : 0
RECORDS RECEIVED        : 0
BATCH STATUS            : Empty batch
```

This was expected because Spark started before the producer sent transactions.

The 2,000 producer messages were then processed across two non-empty Spark micro-batches:

| Spark batch ID | Records received |
|---:|---:|
| 1 | 1 |
| 2 | 1,999 |
| **Total** | **2,000** |

Batch 1 received:

```text
SPARK BATCH ID          : 1
RECORDS RECEIVED        : 1
NULL TRANSACTION IDs    : 0
BATCH STATUS            : Completed
```

Batch 2 received:

```text
SPARK BATCH ID          : 2
RECORDS RECEIVED        : 1999
NULL TRANSACTION IDs    : 0
BATCH STATUS            : Completed
```

The result confirms that:

- Spark consumed all 2,000 Kafka messages;
- the messages were distributed across multiple micro-batches;
- no transaction ID was null;
- both non-empty batches completed;
- no records were lost between the producer and Spark.

---

## MongoDB Validation

MongoDB collection counts were checked after Spark completed the transaction processing:

```powershell
docker exec fraud-mongodb `
    mongosh fraud_detection_db `
    --eval "
print(
    'Prediction history:',
    db.prediction_history.countDocuments()
);
print(
    'Fraud alerts:',
    db.fraud_alerts.countDocuments()
);
"
```

The observed output was:

```text
Prediction history: 3982
Fraud alerts: 25
```

### Interpretation

The `prediction_history` collection stores scored transaction records.

The `fraud_alerts` collection stores transactions that were predicted as fraud.

The results confirm that:

- Spark generated fraud predictions;
- prediction results were written to MongoDB;
- fraud alerts were stored separately;
- the database contained both prediction-history and fraud-alert records.

The pipeline uses transaction identifiers and upsert-based MongoDB operations. Therefore, the total collection count depends on whether a transaction ID is newly inserted or matches an existing record.

---

## Dashboard Functional Review

The dashboard was opened in Grafana using:

```text
http://localhost:3000
```

Dashboard navigation:

```text
Dashboards
└── Fraud Detection
    ├── Fraud Detection Overview
    └── Fraud Pipeline Performance
```

The dashboard was reviewed for the following requirements:

| Requirement | Result |
|---|---|
| Dashboard loaded successfully | Passed |
| Correct dashboard title | Passed |
| Correct dashboard UID | Passed |
| Correct Prometheus datasource | Passed |
| Ten required panels present | Passed |
| Stat panels present | Passed |
| Time-series panels present | Passed |
| Refresh interval set to 5 seconds | Passed |
| Time range set to last 30 minutes | Passed |
| Spark metrics available | Passed |
| MongoDB operation metrics available | Passed |

The dashboard is intended to show:

- the latest Spark batch size;
- the latest batch-processing duration;
- the latest prediction throughput;
- the number of completed Spark batches;
- Spark batch failures;
- prediction failures;
- batch-duration trends;
- throughput trends;
- MongoDB prediction operations;
- MongoDB fraud-alert operations.

---

## Warnings Observed

### Kafka serializer warnings

The producer displayed the following deprecation warnings:

```text
DeprecationWarning: key_serializer does not implement kafka.serializer.Serializer
DeprecationWarning: value_serializer does not implement kafka.serializer.Serializer
```

These warnings did not stop message delivery.

The producer successfully sent all 2,000 transactions.

### Spark configuration warnings

Spark displayed standard warnings related to:

- garbage-collection metrics;
- Kafka consumer configuration;
- Spark runtime configuration.

These warnings did not terminate the streaming query and did not prevent batch completion.

### Empty initial Spark batch

Spark batch 0 contained zero records.

This is normal because the Spark streaming query started before the producer began sending Kafka messages.

---

## Validation Summary

| Validation item | Result |
|---|---|
| Docker environment started | Passed |
| Grafana health check | Passed |
| Dashboard JSON valid | Passed |
| Correct dashboard title | Passed |
| Correct dashboard UID | Passed |
| Correct datasource UID | Passed |
| Ten dashboard panels present | Passed |
| Prometheus port 8000 available | Passed |
| Prometheus metrics endpoint available | Passed |
| Producer sent 2,000 transactions | Passed |
| Producer generated IDs 6001-8000 | Passed |
| Spark received all 2,000 transactions | Passed |
| Batch 1 completed | Passed |
| Batch 2 completed | Passed |
| Null transaction IDs | 0 |
| MongoDB prediction records available | Passed |
| MongoDB fraud alerts available | Passed |
| Fatal pipeline errors | None observed |

---

## End-to-End Flow Validated

The validation demonstrated the following end-to-end processing flow:

```text
PaySim CSV dataset
        ↓
Python Kafka producer
        ↓
Apache Kafka transactions topic
        ↓
Spark Structured Streaming
        ↓
Random Forest fraud prediction
        ↓
MongoDB prediction history and fraud alerts
        ↓
Prometheus operational metrics
        ↓
Grafana performance dashboard
```

Each major component participated successfully in the live test.

---

## Conclusion

The **Fraud Pipeline Performance** dashboard and its supporting monitoring pipeline were functionally validated using a live load of 2,000 transactions.

The producer generated transaction IDs from 6001 through 8000. Spark consumed all 2,000 records across two completed non-empty micro-batches, with no null transaction identifiers. Prediction results and fraud alerts were available in MongoDB, while Spark and database operational metrics were exposed through the Prometheus endpoint.

The dashboard configuration contains the required stat and time-series panels for monitoring batch size, processing duration, throughput, failures, and MongoDB activity.

The performance dashboard is ready for additional large-scale load testing and final project evidence collection.