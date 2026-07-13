# Spark and MongoDB Fraud Pipeline Validation

## 1. Overview

This report documents the successful integration and validation of MongoDB persistence in the Spark fraud-prediction pipeline.

The pipeline now performs the following stages:

```text
PaySim CSV
    |
    v
Kafka Producer
    |
    v
Kafka Topic: transactions
    |
    v
Spark Structured Streaming
    |
    v
JSON Parsing
    |
    v
foreachBatch Processing
    |
    v
Random Forest Prediction
    |
    +--> prediction_history
    |
    +--> fraud_alerts
```

The Spark pipeline file is:

```text
src/streaming/spark_fraud_pipeline.py
```

The MongoDB utility file is:

```text
src/database/connection.py
```

---

## 2. Purpose of the Integration

The earlier Spark pipeline displayed fraud predictions only in the terminal.

The MongoDB integration adds persistent storage so that prediction results remain available after the Spark process stops.

MongoDB now stores:

- every successfully scored transaction;
- the model prediction;
- the fraud probability;
- the original fraud label;
- the Spark batch ID;
- the processing timestamp;
- predicted fraud alerts in a separate collection.

---

## 3. MongoDB Database and Collections

Database:

```text
fraud_detection_db
```

Collections:

```text
prediction_history
fraud_alerts
```

The `prediction_history` collection stores all successfully scored transactions.

The `fraud_alerts` collection stores only transactions where:

```text
prediction = 1
```

---

## 4. MongoDB Connection Validation

The MongoDB helper was tested using:

```powershell
python .\src\database\connection.py
```

Observed output:

```text
MongoDB connection successful.
Database ready: fraud_detection_db
Prediction collection: prediction_history
Fraud-alert collection: fraud_alerts
```

This confirmed that:

- the MongoDB container was reachable;
- the database connection succeeded;
- the required collections were accessible;
- the database indexes were prepared.

---

## 5. Spark Pipeline Command

The Spark pipeline was started from the project root using:

```powershell
python -m src.streaming.spark_fraud_pipeline --reset-checkpoint
```

The module execution format is required because the pipeline imports project modules using the `src` package path.

The checkpoint was reset to create a controlled test run.

---

## 6. Producer Command

The producer was run in a second terminal:

```powershell
python .\src\streaming\producer.py --limit 5 --sleep 0.5
```

The producer streamed five transactions into the Kafka topic:

```text
transactions
```

---

## 7. Spark Startup Result

The pipeline started successfully and displayed:

```text
Spark fraud prediction pipeline started.
Kafka server       : localhost:9092
Kafka topic        : transactions
Processing mode    : foreachBatch
Prediction model   : RandomForestClassifier
MongoDB storage    : Enabled
Prediction history : fraud_detection_db.prediction_history
Fraud alerts       : fraud_detection_db.fraud_alerts
Waiting for transactions...
```

This confirmed that:

- Spark started successfully;
- the Kafka connector loaded;
- the MongoDB connection succeeded;
- model prediction was available;
- MongoDB persistence was enabled.

---

## 8. Batch Processing Results

Spark processed the five transactions across three micro-batches.

### Batch 0

```text
Batch ID          : 0
Records received  : 0
Batch status      : Empty batch
```

The initial empty batch was expected because Spark started before the producer sent transactions.

---

### Batch 1

```text
Batch ID               : 1
Records received       : 1
Null transaction IDs   : 0
Successful predictions : 1
Failed predictions     : 0
Actual fraud labels    : 0
Predicted fraud alerts : 0
True positives         : 0
True negatives         : 1
False positives        : 0
False negatives        : 0
Prediction operations  : 1
Prediction inserts     : 1
Prediction matches     : 0
Fraud alert operations : 0
Fraud alert inserts    : 0
Fraud alert matches    : 0
Processing time        : 73.9930 seconds
Prediction throughput  : 0.01 records/second
Batch status           : Completed
```

Transaction result:

| ID | Type | Amount | Actual | Predicted | Fraud probability |
|---:|---|---:|---:|---:|---:|
| 1 | PAYMENT | 9839.64 | 0 | 0 | 0.000000 |

MongoDB result:

```text
1 prediction-history document inserted
0 fraud-alert documents inserted
```

---

### Batch 2

```text
Batch ID               : 2
Records received       : 4
Null transaction IDs   : 0
Successful predictions : 4
Failed predictions     : 0
Actual fraud labels    : 2
Predicted fraud alerts : 2
True positives         : 2
True negatives         : 2
False positives        : 0
False negatives        : 0
Prediction operations  : 4
Prediction inserts     : 4
Prediction matches     : 0
Fraud alert operations : 2
Fraud alert inserts    : 2
Fraud alert matches    : 0
Processing time        : 2.1635 seconds
Prediction throughput  : 1.85 records/second
Batch status           : Completed
```

Transaction results:

| ID | Type | Amount | Actual | Predicted | Fraud probability |
|---:|---|---:|---:|---:|---:|
| 2 | PAYMENT | 1864.28 | 0 | 0 | 0.000000 |
| 3 | TRANSFER | 181.00 | 1 | 1 | 1.000000 |
| 4 | CASH_OUT | 181.00 | 1 | 1 | 0.986667 |
| 5 | PAYMENT | 11668.14 | 0 | 0 | 0.000000 |

MongoDB result:

```text
4 prediction-history documents inserted
2 fraud-alert documents inserted
```

---

## 9. Overall Prediction Results

Across all non-empty batches:

| Metric | Result |
|---|---:|
| Total transactions sent | 5 |
| Total transactions processed | 5 |
| Successful predictions | 5 |
| Failed predictions | 0 |
| Actual fraudulent transactions | 2 |
| Predicted fraud alerts | 2 |
| True positives | 2 |
| True negatives | 3 |
| False positives | 0 |
| False negatives | 0 |
| Null transaction IDs | 0 |

---

## 10. Overall Confusion Matrix

|  | Predicted Normal | Predicted Fraud |
|---|---:|---:|
| Actual Normal | 3 | 0 |
| Actual Fraud | 0 | 2 |

Therefore:

```text
True positives  = 2
True negatives  = 3
False positives = 0
False negatives = 0
```

For this controlled five-record smoke test:

```text
Accuracy  = 100%
Precision = 100%
Recall    = 100%
F1 score  = 100%
```

These values apply only to this small validation run and must not replace the full model-evaluation results.

---

## 11. MongoDB Prediction-History Validation

The following command was used:

```powershell
docker exec fraud-mongodb mongosh fraud_detection_db --eval "db.prediction_history.find({transaction_id:{`$in:[1,2,3,4,5]}},{_id:0,transaction_id:1,type:1,actual_isFraud:1,prediction:1,fraud_probability:1,spark_batch_id:1}).sort({transaction_id:1}).forEach(printjson);"
```

Observed records:

| Transaction ID | Type | Actual | Prediction | Fraud probability | Spark batch |
|---:|---|---:|---:|---:|---:|
| 1 | PAYMENT | 0 | 0 | 0.000000 | 1 |
| 2 | PAYMENT | 0 | 0 | 0.000000 | 2 |
| 3 | TRANSFER | 1 | 1 | 1.000000 | 2 |
| 4 | CASH_OUT | 1 | 1 | 0.986667 | 2 |
| 5 | PAYMENT | 0 | 0 | 0.000000 | 2 |

This confirmed that all five transactions were persisted successfully.

---

## 12. MongoDB Fraud-Alert Validation

The expected fraud-alert transaction IDs were:

```text
3
4
```

These transactions were correctly routed to:

```text
fraud_detection_db.fraud_alerts
```

Transaction 3:

```text
type = TRANSFER
prediction = 1
fraud_probability = 1.0
```

Transaction 4:

```text
type = CASH_OUT
prediction = 1
fraud_probability = 0.9866666666666667
```

This confirmed that only predicted fraud records were stored in the fraud-alert collection.

---

## 13. Persistence Summary

MongoDB stored:

```text
prediction_history = 5 documents
fraud_alerts        = 2 documents
```

The number of prediction-history documents matched the total number of successfully processed transactions.

The number of fraud-alert documents matched the number of model predictions where:

```text
prediction = 1
```

---

## 14. Upsert and Duplicate-Protection Design

The MongoDB helper uses upsert operations based on:

```text
transaction_id
```

This design prevents the same transaction from being inserted repeatedly during Spark retries or repeated controlled tests.

The persistence behaviour is:

```text
New transaction_id
    |
    v
Insert document

Existing transaction_id
    |
    v
Update existing document
```

This is important because Spark `foreachBatch()` can retry a failed batch.

Without idempotent writes, retries could create duplicate prediction and alert documents.

---

## 15. Cold-Start and Warm-Batch Analysis

Batch 1 required:

```text
73.9930 seconds
```

for one transaction.

Batch 2 required:

```text
2.1635 seconds
```

for four transactions.

The first non-empty batch was much slower because it included cold-start work such as:

- first model inference;
- model and feature preparation;
- Python-Java communication initialization;
- first MongoDB bulk operation;
- Spark task startup;
- local Windows file and process initialization.

Batch 2 represents warm execution after the main components had already initialized.

The warm-batch throughput was:

```text
1.85 records/second
```

The current result is a functional validation result, not a production throughput benchmark.

---

## 16. Important Learning: Producer Completion Is Not Final Validation

The producer output:

```text
Streaming completed. Total transactions sent: 5
```

only confirms that five records were sent to Kafka.

A complete pipeline validation also requires confirmation from:

- Spark batch output;
- prediction results;
- MongoDB write metrics;
- MongoDB collection queries.

The successful end-to-end validation chain is:

```text
Producer sent records
        |
        v
Kafka accepted records
        |
        v
Spark processed records
        |
        v
Model generated predictions
        |
        v
MongoDB stored results
```

---

## 17. Important Learning: Spark Must Finish the Batch

The Spark process must not be stopped immediately after:

```text
RECORDS RECEIVED
```

MongoDB writes occur later in the `foreachBatch()` function.

The batch must reach:

```text
MongoDB persistence
BATCH STATUS : Completed
```

before the MongoDB collections are checked.

---

## 18. Important Learning: Micro-Batch Distribution

The five producer records were distributed as:

```text
Batch 1: 1 record
Batch 2: 4 records
```

This is normal Spark Structured Streaming behaviour.

The distribution depends on:

- Kafka arrival timing;
- Spark trigger timing;
- Kafka partitions;
- consumer polling;
- local processing speed.

Validation should compare the total records across all batches rather than expecting all records in one batch.

---

## 19. Successful Validation Checklist

| Validation | Result |
|---|---|
| MongoDB container | Passed |
| MongoDB connection | Passed |
| Database indexes | Passed |
| Spark startup | Passed |
| Kafka connector loading | Passed |
| Kafka transaction consumption | Passed |
| JSON parsing | Passed |
| `foreachBatch()` processing | Passed |
| Random Forest prediction | Passed |
| Fraud probability generation | Passed |
| Prediction-history persistence | Passed |
| Fraud-alert persistence | Passed |
| True-positive calculation | Passed |
| True-negative calculation | Passed |
| False-positive calculation | Passed |
| False-negative calculation | Passed |
| Processing-time measurement | Passed |
| Throughput calculation | Passed |
| Five-record end-to-end test | Passed |

---

## 20. Current Pipeline Status

The project now supports:

```text
Kafka transaction streaming
Spark Structured Streaming
JSON schema parsing
foreachBatch processing
Random Forest prediction
Fraud probability generation
Batch validation metrics
MongoDB prediction history
MongoDB fraud alerts
Idempotent MongoDB upserts
```

---

## 21. Next Development Stage

The next appropriate project stage is monitoring and observability.

The planned architecture is:

```text
Kafka
   |
   v
Spark Fraud Pipeline
   |
   +--> MongoDB
   |
   +--> Prometheus Metrics
              |
              v
           Grafana
```

Prometheus will collect operational metrics.

Grafana will display those metrics in a dashboard.

Planned metrics include:

- total processed transactions;
- total predicted fraud alerts;
- total failed predictions;
- batch count;
- batch processing time;
- prediction throughput;
- MongoDB prediction writes;
- MongoDB fraud-alert writes;
- true positives;
- true negatives;
- false positives;
- false negatives.

---

## 22. Final Result

The Spark and MongoDB integration passed the controlled end-to-end validation.

The pipeline successfully:

- consumed five Kafka transactions;
- generated five Random Forest predictions;
- produced fraud probabilities;
- correctly detected two fraud transactions;
- persisted all five predictions;
- persisted two fraud alerts;
- produced zero failed predictions;
- produced zero false positives;
- produced zero false negatives;
- recorded Spark batch IDs in MongoDB;
- completed both non-empty batches successfully.

The next milestone is Prometheus and Grafana monitoring.