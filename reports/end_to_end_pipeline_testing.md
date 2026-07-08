# End-to-End Pipeline Testing Report

## Purpose

This report documents the testing of the real-time fraud detection pipeline.

The goal of this test was to confirm that transaction data successfully moves through the complete pipeline:

```text
Producer → Kafka → Consumer → Model Prediction → MongoDB
```

This testing proves that the project is not only training a fraud detection model, but also using the saved model in a working real-time-style pipeline.

---

## Pipeline Flow Tested

```text
PaySim CSV Dataset
        ↓
Kafka Producer
        ↓
Kafka Topic: transactions
        ↓
Kafka Consumer
        ↓
Saved Random Forest Model
        ↓
MongoDB prediction_history
        ↓
MongoDB fraud_alerts
```

---

## Files Involved

- `src/streaming/producer.py`  
  Sends transaction records from the CSV dataset to Kafka.

- `src/streaming/consumer.py`  
  Reads transactions from Kafka, predicts fraud, and stores results in MongoDB.

- `src/models/predict_fraud.py`  
  Loads the saved Random Forest model and prepares each transaction for prediction.

- `src/database/connection.py`  
  Connects the project to MongoDB and stores prediction history and fraud alerts.

---

## Commands Used for Testing

### 1. Start Docker services

```powershell
docker compose -f docker/docker-compose.yml up -d
```

This starts Kafka, Kafka UI, MongoDB, and Mongo Express.

---

### 2. Check running containers

```powershell
docker ps
```

Expected containers:

```text
fraud-kafka
fraud-kafka-ui
fraud-mongodb
fraud-mongo-express
```

---

### 3. Check Kafka topic

```powershell
docker exec -it fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

Expected topic:

```text
transactions
```

---

### 4. Test MongoDB connection

```powershell
python src\database\connection.py
```

Expected output:

```text
MongoDB connection successful.
Database ready: fraud_detection_db
```

---

### 5. Run Kafka consumer

```powershell
python src\streaming\consumer.py --group-id full-test-1 --limit 100 --threshold 0.5
```

The consumer waits for transactions from Kafka, predicts fraud, and saves results into MongoDB.

---

### 6. Run Kafka producer

```powershell
python src\streaming\producer.py --limit 100 --sleep 0.01
```

Expected output:

```text
Streaming completed. Total transactions sent: 100
```

---

## MongoDB Collections Verified

The following MongoDB database was checked:

```text
fraud_detection_db
```

Collections checked:

```text
prediction_history
fraud_alerts
```

---

## Prediction History Output

Exported file:

```text
prediction_history.csv
```

Shape of the file:

```text
200 rows × 21 columns
```

This means 200 prediction records were stored in MongoDB.

### Columns in `prediction_history.csv`

```text
_id
step
type
amount
nameOrig
oldbalanceOrg
newbalanceOrig
nameDest
oldbalanceDest
newbalanceDest
isFraud
isFlaggedFraud
transaction_id
stream_timestamp
prediction
fraud_probability
threshold
model_used
kafka_partition
kafka_offset
processed_at
```

### Meaning of important columns

- `type` shows the transaction type, such as PAYMENT, TRANSFER, CASH_OUT, or DEBIT.
- `amount` shows the transaction amount.
- `isFraud` is the actual label from the dataset.
- `prediction` is the model output.
- `fraud_probability` is the model confidence score.
- `threshold` is the cutoff used for fraud prediction.
- `model_used` shows the model used for prediction.
- `processed_at` shows when the record was stored in MongoDB.
- `kafka_partition` and `kafka_offset` show where the message was read from in Kafka.

---

## Prediction Summary

| Metric | Value |
|---|---:|
| Total records | 200 |
| Predicted non-fraud | 198 |
| Predicted fraud | 2 |
| Actual non-fraud | 198 |
| Actual fraud | 2 |
| False positives | 0 |
| False negatives | 0 |
| Model used | RandomForestClassifier |
| Threshold | 0.5 |

### Analysis

- The pipeline processed 200 transactions successfully.
- The model predicted 2 transactions as fraud.
- The dataset also had 2 actual fraud transactions in this exported output.
- Both actual fraud transactions were correctly predicted as fraud.
- No false positives were found.
- No false negatives were found.
- This shows that the tested pipeline output is correct for this sample.

---

## Transaction Type Summary

| Transaction Type | Count |
|---|---:|
| PAYMENT | 111 |
| TRANSFER | 46 |
| CASH_OUT | 22 |
| DEBIT | 21 |

### Analysis

- Most transactions in this test output were PAYMENT transactions.
- Fraud predictions appeared only in TRANSFER and CASH_OUT transactions.
- This matches the PaySim dataset behavior because fraud usually appears in TRANSFER and CASH_OUT transactions.

---

## Fraud Prediction Records

| Transaction ID | Type | Amount | Actual Fraud | Prediction | Fraud Probability |
|---:|---|---:|---:|---:|---:|
| 3 | TRANSFER | 181.00 | 1 | 1 | 1.000000 |
| 4 | CASH_OUT | 181.00 | 1 | 1 | 0.986667 |

### Analysis

- Transaction ID 3 was a TRANSFER transaction and was predicted as fraud with probability `1.000000`.
- Transaction ID 4 was a CASH_OUT transaction and was predicted as fraud with probability `0.986667`.
- Both records were actual fraud cases and were correctly detected by the model.

---

## Fraud Alerts Output

Exported file:

```text
fraud_alerts.csv
```

Shape of the file:

```text
3 rows × 17 columns
```

Important note:

One row is a MongoDB test row:

```text
MongoDB connected successfully
```

Actual fraud alert records:

```text
2
```

### Columns in `fraud_alerts.csv`

```text
_id
test
message
created_at
transaction_id
transaction_type
amount
nameOrig
nameDest
actual_isFraud
prediction
fraud_probability
threshold
model_used
kafka_partition
kafka_offset
alert_status
```

---

## Fraud Alerts Summary

| Transaction ID | Type | Amount | Prediction | Fraud Probability | Alert Status |
|---:|---|---:|---:|---:|---|
| 3 | TRANSFER | 181.00 | 1 | 1.000000 | OPEN |
| 4 | CASH_OUT | 181.00 | 1 | 0.986667 | OPEN |

### Analysis

- The `fraud_alerts` collection stores only high-risk fraud predictions.
- Both actual fraud alerts are marked as `OPEN`.
- In a real system, these alerts could later be reviewed by a fraud analyst.
- Alert status can later be updated to values like `REVIEWED`, `CONFIRMED`, or `CLOSED`.

---

## Browser Verification

### Kafka UI

Kafka UI can be opened at:

```text
http://localhost:8080
```

Use it to check:

- Kafka cluster
- `transactions` topic
- partitions
- messages
- consumer groups

Kafka UI is mainly for checking Kafka activity. It does not run the ML model.

---

### Mongo Express

Mongo Express can be opened at:

```text
http://localhost:8081
```

Use it to check:

- `fraud_detection_db`
- `prediction_history`
- `fraud_alerts`
- stored documents
- prediction output

Mongo Express is useful for viewing MongoDB records without using terminal commands.

---

## Final Testing Status

Final status:

```text
End-to-end pipeline test completed successfully.
```

Confirmed:

- Producer sent transaction data to Kafka.
- Kafka topic received transaction messages.
- Consumer read messages from Kafka.
- Saved Random Forest model generated predictions.
- Prediction history was stored in MongoDB.
- Fraud alerts were stored separately in MongoDB.
- Exported CSV files confirmed the pipeline output.

---

## Final Learning

This test confirms that the project has moved from offline model training to a working real-time prediction pipeline.

The main learning is:

```text
Kafka handles streaming.
The consumer connects streaming data with the ML model.
The saved Random Forest model predicts fraud.
MongoDB stores both prediction history and fraud alerts.
Kafka UI and Mongo Express help verify the system visually.
```