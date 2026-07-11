# Spark Fraud Pipeline Progress

## 1. Overview

The project has progressed from Kafka transaction streaming to Spark Structured Streaming and model-based fraud prediction.

The main files are:

```text
src/streaming/producer.py
src/streaming/spark_streaming_consumer.py
src/streaming/spark_fraud_pipeline.py
src/models/predict_fraud.py
```

---

## 2. Completed and Validated Work

The following stages have been completed and validated on the primary development system:

- Kafka producer implementation;
- streaming transaction data into the `transactions` topic;
- Spark Structured Streaming integration;
- explicit JSON-schema parsing;
- `foreachBatch()` processing;
- transaction-count validation;
- null transaction-ID validation;
- fraud-label counting;
- Spark checkpoint support;
- batch processing-time measurement;
- throughput calculation.

A controlled test successfully processed 20 transactions across Spark micro-batches.

---

## 3. Current Pipeline

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
Batch Validation and Metrics
```

---

## 4. Current Batch Metrics

The validated `foreachBatch()` pipeline reports:

- batch ID;
- record count;
- null transaction-ID count;
- fraud-label count;
- processing time;
- throughput;
- batch status.

---

## 5. Random Forest Integration

The fraud-prediction stage has been implemented in the Spark pipeline code.

The intended flow is:

```text
Kafka Transaction
      |
      v
Spark Micro-batch
      |
      v
Reusable Prediction Helper
      |
      v
Feature Preparation
      |
      v
Random Forest Model
      |
      +--> Fraud Prediction
      |
      +--> Fraud Probability
      |
      +--> Prediction Metrics
```

The pipeline is designed to:

- reuse `src/models/predict_fraud.py`;
- load the trained Random Forest model;
- use the saved feature list;
- generate binary fraud predictions;
- generate fraud probabilities;
- compare model predictions with PaySim labels;
- calculate true positives;
- calculate true negatives;
- calculate false positives;
- calculate false negatives;
- measure prediction throughput.

---

## 6. Teammate Validation Status

The teammate attempted to run:

```powershell
python .\src\streaming\spark_fraud_pipeline.py --reset-checkpoint
```

The following components worked:

| Validation | Result |
|---|---|
| PySpark startup | Passed |
| Spark Kafka package resolution | Passed |
| Spark Kafka connector `3.5.1` | Passed |
| Kafka client dependency resolution | Passed |
| Hadoop client dependency resolution | Passed |

Spark initialization then failed because the local Hadoop environment was not configured:

```text
HADOOP_HOME and hadoop.home.dir are unset
```

Therefore, the following stages were not reached during that run:

| Validation | Result |
|---|---|
| Spark session initialization | Failed |
| Kafka stream connection | Not tested |
| JSON parsing | Not tested |
| `foreachBatch()` execution | Not tested |
| Random Forest prediction | Not tested |
| Fraud probability generation | Not tested |
| Throughput validation | Not tested |

The next teammate test should be performed after setting `HADOOP_HOME` and adding the Hadoop `bin` directory to `PATH`.

---

## 7. Hadoop Fix Required for Teammate System

Example commands:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "C:\hadoop\bin;$env:Path"
```

Verification:

```powershell
Test-Path C:\hadoop\bin\winutils.exe
Test-Path C:\hadoop\bin\hadoop.dll
where.exe winutils
```

After these checks pass, rerun:

```powershell
python .\src\streaming\spark_fraud_pipeline.py --reset-checkpoint
```

---

## 8. Planned Validation Test

After Spark starts successfully, run a five-record smoke test:

```powershell
python .\src\streaming\producer.py --limit 5 --sleep 0.5
```

A successful result should confirm:

- five total transactions received across one or more batches;
- zero failed predictions;
- predictions generated;
- fraud probabilities generated;
- probabilities between 0 and 1;
- processing time displayed;
- throughput displayed;
- batch status completed.

---

## 9. Next Stage: MongoDB Integration

After Spark model inference is validated, the next stage will be:

```text
Kafka
   |
   v
Spark Structured Streaming
   |
   v
Feature Preparation
   |
   v
Random Forest Model
   |
   v
Prediction and Fraud Probability
   |
   v
MongoDB
   |
   +--> prediction_history
   |
   +--> fraud_alerts
```

---

## 10. MongoDB Storage Plan

The `prediction_history` collection will store all scored transactions.

Planned fields include:

- transaction ID;
- transaction type;
- amount;
- original PaySim fraud label;
- predicted class;
- fraud probability;
- model name;
- Spark batch ID;
- stream timestamp;
- prediction timestamp.

The `fraud_alerts` collection will store transactions where:

```text
prediction = 1
```

MongoDB writes should use batch insertion or upsert logic and duplicate protection.

---

## 11. Current Project Status

| Stage | Status |
|---|---|
| Kafka producer | Completed |
| Kafka transaction streaming | Completed |
| Spark Kafka consumer | Completed |
| JSON parsing | Completed |
| `foreachBatch()` processing | Completed |
| Batch metrics | Completed |
| Random Forest integration in code | Implemented |
| Teammate prediction validation | In progress |
| MongoDB storage from Spark | Planned |
| Prometheus metrics | Planned |
| Grafana dashboard | Planned |

---

## 12. Conclusion

The Kafka-to-Spark streaming foundation is complete and validated.

The model-inference logic has been added to the Spark fraud pipeline, but the teammate’s local validation is still in progress because Spark could not start without `HADOOP_HOME`.

The immediate next action is to correct the Hadoop environment, rerun the five-record smoke test and record the prediction results accurately.