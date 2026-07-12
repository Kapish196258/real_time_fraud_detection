# Spark Fraud Prediction Pipeline Validation

## Overview

This report documents the successful validation of the Spark fraud-prediction pipeline.

The pipeline consumes transaction events from Kafka using Spark Structured Streaming, processes each micro-batch through `foreachBatch()`, applies the trained Random Forest model and displays fraud predictions, fraud probabilities and batch-level performance metrics.

The validated pipeline file is:

```text
src/streaming/spark_fraud_pipeline.py
```

The reusable prediction helper is:

```text
src/models/predict_fraud.py
```

---

## Pipeline Architecture

```text
PaySim Transaction Data
          |
          v
Python Kafka Producer
          |
          v
Kafka Topic: transactions
          |
          v
Spark Structured Streaming
          |
          v
JSON Schema Parsing
          |
          v
foreachBatch Processing
          |
          v
Random Forest Prediction Helper
          |
          +--> Predicted Fraud Class
          |
          +--> Fraud Probability
          |
          +--> Confusion-Matrix Metrics
          |
          +--> Processing-Time Metrics
```

---

## Technologies Used

| Component | Purpose |
|---|---|
| Python | Producer, model helper and pipeline execution |
| PySpark 3.5.1 | Spark Structured Streaming processing |
| Apache Spark 3.5.1 | Micro-batch stream processing |
| Apache Kafka | Transaction event transport |
| Docker | Local Kafka and supporting services |
| Random Forest | Fraud classification model |
| Joblib | Saved-model loading |
| PaySim | Transaction dataset and fraud labels |

The Spark Kafka package used by the pipeline is:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
```

---

## Validation Commands

### Test the prediction helper

```powershell
python .\src\models\predict_fraud.py
```

Observed result:

```text
{
    'transaction_id': 1,
    'prediction': 1,
    'fraud_probability': 1.0,
    'threshold': 0.5,
    'model_used': 'RandomForestClassifier'
}
```

This confirmed that:

- the saved Random Forest model loaded successfully;
- the saved feature list loaded successfully;
- transaction feature preparation worked;
- a fraud prediction was generated;
- a fraud probability was returned.

---

### Start the Spark pipeline

The Spark pipeline must be executed as a Python module from the project root:

```powershell
python -m src.streaming.spark_fraud_pipeline --reset-checkpoint
```

Using module execution ensures that Python can resolve imports beginning with:

```text
src.
```

The direct-file command below should not be used for this pipeline:

```powershell
python .\src\streaming\spark_fraud_pipeline.py --reset-checkpoint
```

Running the file directly may produce:

```text
ModuleNotFoundError: No module named 'src'
```

---

### Produce the validation transactions

In a second terminal:

```powershell
python .\src\streaming\producer.py --limit 5 --sleep 0.5
```

The producer streamed five PaySim transactions into the Kafka topic:

```text
transactions
```

---

## Spark Startup Result

The pipeline started successfully and reported:

```text
Spark fraud prediction pipeline started.
Kafka server       : localhost:9092
Kafka topic        : transactions
Processing mode    : foreachBatch
Prediction model   : RandomForestClassifier
MongoDB storage    : Not enabled in this stage
Waiting for transactions...
```

This confirmed that:

- Spark initialized successfully;
- the Kafka connector loaded successfully;
- the pipeline subscribed to the correct Kafka broker;
- the `transactions` topic was configured;
- the Random Forest prediction helper loaded;
- checkpoint reset completed;
- the stream entered the waiting state.

---

## Batch Processing Results

Spark processed the five transactions across three micro-batches.

### Batch 0

```text
Batch ID          : 0
Records received  : 0
Batch status      : Empty batch
```

The initial empty batch is expected Spark Structured Streaming behaviour.

The stream started before any new Kafka messages were available, so Spark created an empty micro-batch while initializing the streaming query.

This is not an error.

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
Processing time        : 16.6787 seconds
Prediction throughput  : 0.06 records/second
Batch status           : Completed
```

Transaction result:

| ID | Type | Amount | Actual | Predicted | Fraud probability |
|---:|---|---:|---:|---:|---:|
| 1 | PAYMENT | 9839.64 | 0 | 0 | 0.000000 |

The normal payment transaction was classified correctly.

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
Processing time        : 1.4885 seconds
Prediction throughput  : 2.69 records/second
Batch status           : Completed
```

Transaction results:

| ID | Type | Amount | Actual | Predicted | Fraud probability |
|---:|---|---:|---:|---:|---:|
| 2 | PAYMENT | 1864.28 | 0 | 0 | 0.000000 |
| 3 | TRANSFER | 181.00 | 1 | 1 | 1.000000 |
| 4 | CASH_OUT | 181.00 | 1 | 1 | 0.986667 |
| 5 | PAYMENT | 11668.14 | 0 | 0 | 0.000000 |

Both fraudulent transactions were detected correctly.

Both normal transactions were classified correctly.

---

## Overall Validation Summary

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

## Overall Confusion Matrix

|  | Predicted Normal | Predicted Fraud |
|---|---:|---:|
| Actual Normal | 3 | 0 |
| Actual Fraud | 0 | 2 |

Therefore:

```text
True Positives  = 2
True Negatives  = 3
False Positives = 0
False Negatives = 0
```

---

## Validation Metrics

For this five-record smoke test:

### Accuracy

```text
Accuracy = (TP + TN) / Total
Accuracy = (2 + 3) / 5
Accuracy = 1.00
```

```text
Accuracy = 100%
```

### Precision

```text
Precision = TP / (TP + FP)
Precision = 2 / (2 + 0)
Precision = 1.00
```

```text
Precision = 100%
```

### Recall

```text
Recall = TP / (TP + FN)
Recall = 2 / (2 + 0)
Recall = 1.00
```

```text
Recall = 100%
```

### F1 Score

```text
F1 = 2 × (Precision × Recall) / (Precision + Recall)
F1 = 1.00
```

```text
F1 Score = 100%
```

These values describe only the five-record smoke test and should not be treated as the final model-evaluation metrics.

The full model performance must continue to be reported using the complete test dataset.

---

## Fraud Probability Analysis

The model produced probabilities between 0 and 1 for every transaction.

### Normal transactions

The three payment transactions produced:

```text
0.000000
```

fraud probability.

They were therefore classified as normal.

### Fraudulent transfer

Transaction 3 produced:

```text
1.000000
```

fraud probability.

The transaction was classified as fraud with maximum model confidence.

### Fraudulent cash-out

Transaction 4 produced:

```text
0.986667
```

fraud probability.

The transaction was classified as fraud with approximately:

```text
98.67%
```

estimated fraud probability.

The output confirms that the pipeline returns both:

- a binary fraud decision;
- a continuous fraud-risk score.

---

## Processing-Time Analysis

Batch 1 processed one record in:

```text
16.6787 seconds
```

Batch 2 processed four records in:

```text
1.4885 seconds
```

The first non-empty batch was significantly slower than the second batch.

This is expected because the first prediction batch can include one-time startup costs such as:

- Python module initialization;
- model deserialization;
- feature-list loading;
- Spark worker preparation;
- Java-to-Python communication setup;
- first-time execution of prediction logic;
- local file-access initialization.

After initialization, the second batch processed four records much faster.

This behaviour demonstrates the difference between:

```text
cold-start performance
```

and:

```text
warm-batch performance
```

For operational monitoring, later warm batches provide a more representative measurement of steady-state processing speed.

---

## Throughput Analysis

Batch 1 throughput:

```text
0.06 records/second
```

Batch 2 throughput:

```text
2.69 records/second
```

The second batch achieved substantially higher throughput because the model and prediction environment had already been initialized.

The current test uses:

- a local Windows machine;
- a very small transaction count;
- per-record Python prediction calls;
- no production cluster;
- no batch vectorization;
- no distributed model serving.

Therefore, the measured throughput is a functional validation result rather than a production benchmark.

---

## Successful Validation Checks

| Validation check | Result |
|---|---|
| Prediction helper execution | Passed |
| Saved Random Forest loading | Passed |
| Saved feature loading | Passed |
| Spark session initialization | Passed |
| Spark Kafka connector loading | Passed |
| Kafka connection | Passed |
| Kafka transaction consumption | Passed |
| JSON parsing | Passed |
| `foreachBatch()` execution | Passed |
| Transaction-ID validation | Passed |
| Fraud prediction generation | Passed |
| Fraud probability generation | Passed |
| Successful prediction counting | Passed |
| Failed prediction counting | Passed |
| Confusion-matrix calculation | Passed |
| Processing-time measurement | Passed |
| Throughput calculation | Passed |
| Checkpoint reset | Passed |
| Five-record smoke test | Passed |

---

## Important Learning: Python Module Execution

The pipeline imports the prediction module using the project package path:

```python
src.models.predict_fraud
```

For this reason, the pipeline must be launched from the repository root using:

```powershell
python -m src.streaming.spark_fraud_pipeline --reset-checkpoint
```

The `-m` option tells Python to execute the script as part of the project package.

This keeps the repository root on Python's import path and allows imports beginning with `src` to work correctly.

---

## Important Learning: Empty Spark Batch

Spark produced an initial batch with zero records:

```text
SPARK BATCH ID : 0
RECORDS RECEIVED : 0
```

This is normal for a newly started streaming query.

An empty batch can occur when:

- Spark starts before the producer;
- no new Kafka offsets are available;
- the trigger runs before messages arrive;
- checkpoint state is initialized.

The pipeline correctly identified the batch as empty and returned without attempting model inference.

---

## Important Learning: Micro-Batch Distribution

The producer sent five transactions, but Spark divided them as:

```text
Batch 1: 1 record
Batch 2: 4 records
```

Spark does not guarantee that all produced transactions will appear in one micro-batch.

Batch distribution depends on:

- transaction arrival timing;
- Spark trigger timing;
- Kafka polling;
- partition availability;
- local processing speed.

Validation should therefore use the total number of records across all completed batches rather than expecting all records in one batch.

---

## Current Limitation

The current pipeline only prints prediction results and batch metrics to the terminal.

MongoDB storage is not enabled in this stage.

The pipeline currently reports:

```text
MongoDB storage : Not enabled in this stage
```

Therefore, prediction results are not yet persisted after the Spark process stops.

---

## 18. Next Development Stage

The next stage is to store Spark prediction results in MongoDB.

The planned architecture is:

```text
Kafka Transactions
        |
        v
Spark Structured Streaming
        |
        v
Random Forest Prediction
        |
        +----------------------+
        |                      |
        v                      v
prediction_history        fraud_alerts
```

The `prediction_history` collection will store every successfully scored transaction.

The `fraud_alerts` collection will store transactions where:

```text
prediction = 1
```

---

## Planned MongoDB Fields

Each prediction-history document should contain fields such as:

```text
transaction_id
type
amount
actual_isFraud
prediction
fraud_probability
threshold
model_used
spark_batch_id
stream_timestamp
prediction_timestamp
```

Fraud-alert documents should additionally support:

```text
alert_status
alert_created_at
risk_level
```

---

## Final Result

The Spark fraud-prediction stage passed the controlled five-record smoke test.

The pipeline successfully:

- consumed Kafka transactions;
- parsed transaction JSON;
- processed data using `foreachBatch()`;
- loaded and reused the trained Random Forest model;
- generated fraud predictions;
- generated fraud probabilities;
- detected both fraudulent transactions;
- correctly classified all normal transactions;
- produced zero failed predictions;
- calculated confusion-matrix values;
- measured processing time;
- calculated prediction throughput.

The next technical milestone is MongoDB prediction and fraud-alert storage.