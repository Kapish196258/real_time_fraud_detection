# MongoDB Queries Report

## Project Name

Real-Time Financial Fraud Detection Pipeline

## Purpose

This report documents the MongoDB queries used to verify and analyze prediction results stored by the real-time fraud detection pipeline.

MongoDB is used as the storage layer of the project. After transactions are streamed through Kafka and predicted by the saved Random Forest model, the prediction results are stored in MongoDB.

The goal of these queries is to confirm that the real-time pipeline is working and to extract useful fraud detection insights.

---

## Database and Collections

Database used:

```text
fraud_detection_db
```

Collections used:

```text
prediction_history
fraud_alerts
```

### `prediction_history`

Stores all transactions processed by the Kafka consumer.

It contains both fraud and non-fraud predictions.

### `fraud_alerts`

Stores only transactions predicted as fraud.

---

## Final Clean Test Data

For final MongoDB query testing, old development records were cleared and a clean test stream was processed.

Final test size:

| Item | Count |
|---|---:|
| Total prediction records | 2,000 |
| Total fraud alerts | 18 |
| Predicted non-fraud records | 1,982 |
| Predicted fraud records | 18 |

This means the end-to-end pipeline successfully processed 2,000 transactions and generated 18 fraud alerts.

---

## How to Open MongoDB Shell

Command used:

```powershell
docker exec -it fraud-mongodb mongosh fraud_detection_db
```

This opens the MongoDB shell directly inside the `fraud_detection_db` database.

---

## Query 1: Count Total Prediction Records

### Query

```javascript
db.prediction_history.countDocuments()
```

### Output

```text
2000
```

### Understanding

This confirms that 2,000 transaction prediction records were successfully stored in MongoDB.

This means the Kafka consumer was able to read transactions, run fraud prediction, and save the results.

---

## Query 2: Count Total Fraud Alerts

### Query

```javascript
db.fraud_alerts.countDocuments()
```

### Output

```text
18
```

### Understanding

This confirms that 18 transactions were predicted as fraud and saved separately in the `fraud_alerts` collection.

The separate fraud alert collection is useful because high-risk transactions can be monitored directly without scanning the full prediction history.

---

## Query 3: Latest 5 Prediction Records

### Query

```javascript
db.prediction_history.find(
  {},
  {
    _id: 0,
    transaction_id: 1,
    type: 1,
    amount: 1,
    isFraud: 1,
    prediction: 1,
    fraud_probability: 1,
    processed_at: 1
  }
).sort({ processed_at: -1 }).limit(5)
```

### Output Summary

| transaction_id | type | amount | actual isFraud | prediction | fraud_probability |
|---:|---|---:|---:|---:|---:|
| 1580 | PAYMENT | 1117.17 | 0 | 0 | 0 |
| 1577 | PAYMENT | 1667.88 | 0 | 0 | 0 |
| 1574 | CASH_IN | 96015.94 | 0 | 0 | 0 |
| 1566 | PAYMENT | 727.94 | 0 | 0 | 0 |
| 1565 | DEBIT | 5655.43 | 0 | 0 | 0 |

### Understanding

This query shows the most recently processed prediction records.

The latest records shown here were all non-fraud predictions with fraud probability `0`.

This confirms that prediction records are being saved with important fields such as transaction ID, transaction type, amount, actual fraud label, predicted label, probability, and timestamp.

---

## Query 4: Top 10 Highest Fraud Probability Transactions

### Query

```javascript
db.prediction_history.find(
  {},
  {
    _id: 0,
    transaction_id: 1,
    type: 1,
    amount: 1,
    isFraud: 1,
    prediction: 1,
    fraud_probability: 1,
    processed_at: 1
  }
).sort({ fraud_probability: -1 }).limit(10)
```

### Output Summary

| transaction_id | type | amount | actual isFraud | prediction | fraud_probability |
|---:|---|---:|---:|---:|---:|
| 3 | TRANSFER | 181.00 | 1 | 1 | 1.0000 |
| 3 | TRANSFER | 181.00 | 1 | 1 | 1.0000 |
| 3 | TRANSFER | 181.00 | 1 | 1 | 1.0000 |
| 3 | TRANSFER | 181.00 | 1 | 1 | 1.0000 |
| 252 | TRANSFER | 2806.00 | 1 | 1 | 1.0000 |
| 681 | TRANSFER | 20128.00 | 1 | 1 | 1.0000 |
| 970 | TRANSFER | 1277212.77 | 1 | 1 | 1.0000 |
| 682 | CASH_OUT | 20128.00 | 1 | 1 | 0.9933 |
| 4 | CASH_OUT | 181.00 | 1 | 1 | 0.9867 |
| 4 | CASH_OUT | 181.00 | 1 | 1 | 0.9867 |

### Understanding

This query shows the highest-risk transactions according to the model.

The top fraud probability transactions are mainly `TRANSFER` and `CASH_OUT` transactions. This matches the PaySim dataset behavior, where fraud mainly appears in these two transaction types.

Some transaction IDs appear more than once because this is a streaming test environment and repeated Kafka test messages can appear during development runs. For final dashboarding, this can be handled by using a clean Kafka topic or adding a unique run ID.

---

## Query 5: Fraud vs Non-Fraud Prediction Count

### Query

```javascript
db.prediction_history.aggregate([
  {
    $group: {
      _id: "$prediction",
      count: { $sum: 1 }
    }
  },
  {
    $sort: { _id: 1 }
  }
])
```

### Output

| prediction | Meaning | Count |
|---:|---|---:|
| 0 | Predicted non-fraud | 1,982 |
| 1 | Predicted fraud | 18 |

### Understanding

Out of 2,000 processed transactions:

- 1,982 were predicted as non-fraud.
- 18 were predicted as fraud.

This confirms that the prediction output is highly imbalanced, which is expected in fraud detection because fraud cases are rare.

---

## Query 6: Actual vs Predicted Comparison

### Query

```javascript
db.prediction_history.aggregate([
  {
    $group: {
      _id: {
        actual: "$isFraud",
        predicted: "$prediction"
      },
      count: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.actual": 1,
      "_id.predicted": 1
    }
  }
])
```

### Output

| Actual isFraud | Predicted | Meaning | Count |
|---:|---:|---|---:|
| 0 | 0 | True Negative | 1,982 |
| 1 | 1 | True Positive | 18 |

### Understanding

This result is very strong for this clean test sample.

It means:

- 1,982 normal transactions were correctly predicted as non-fraud.
- 18 fraud transactions were correctly predicted as fraud.
- No false positives appeared in this sample.
- No false negatives appeared in this sample.

For fraud detection, false negatives are especially important because they mean actual fraud was missed. In this test output, no false negatives were found.

---

## Query 7: Fraud Alerts by Transaction Type

### Query

```javascript
db.fraud_alerts.aggregate([
  {
    $group: {
      _id: "$transaction_type",
      alert_count: { $sum: 1 }
    }
  },
  {
    $sort: { alert_count: -1 }
  }
])
```

### Output

| Transaction Type | Fraud Alert Count |
|---|---:|
| CASH_OUT | 11 |
| TRANSFER | 7 |

### Understanding

Fraud alerts appeared only in:

```text
CASH_OUT
TRANSFER
```

This is an important business and data insight because it shows that risky transactions are concentrated in specific transaction types.

In a real monitoring system, these transaction types can be watched more closely.

---

## Query 8: Fraud Alert Amount Summary

### Query

```javascript
db.fraud_alerts.aggregate([
  {
    $group: {
      _id: null,
      total_alerts: { $sum: 1 },
      average_amount: { $avg: "$amount" },
      min_amount: { $min: "$amount" },
      max_amount: { $max: "$amount" }
    }
  },
  {
    $project: {
      _id: 0,
      total_alerts: 1,
      average_amount: { $round: ["$average_amount", 2] },
      min_amount: 1,
      max_amount: 1
    }
  }
])
```

### Output

| Metric | Value |
|---|---:|
| Total fraud alerts | 18 |
| Minimum fraud alert amount | 181 |
| Maximum fraud alert amount | 1,277,212.77 |
| Average fraud alert amount | 169,766.31 |

### Understanding

This query gives amount-level insight about fraud alerts.

The maximum fraud alert amount was very high, which shows why fraud detection is important. A single high-value fraud transaction can create serious financial loss.

The average fraud alert amount was also high, which means fraud alerts are not only frequent-risk events but can also carry financial impact.

---

## Query 9: False Negative Check

### Query

```javascript
db.prediction_history.find(
  {
    isFraud: 1,
    prediction: 0
  },
  {
    _id: 0,
    transaction_id: 1,
    type: 1,
    amount: 1,
    fraud_probability: 1
  }
).limit(10)
```

### Output

```text
No records returned
```

### Understanding

This means no false negatives were found in this test sample.

A false negative means:

```text
Actual fraud = 1
Prediction = 0
```

In simple terms, it means the model missed a fraud transaction.

For fraud detection, reducing false negatives is very important because missing fraud can create financial loss.

---

## Query 10: Fraud Probability Buckets

### Query

```javascript
db.prediction_history.aggregate([
  {
    $bucket: {
      groupBy: "$fraud_probability",
      boundaries: [0, 0.25, 0.5, 0.75, 1.01],
      default: "unknown",
      output: {
        count: { $sum: 1 }
      }
    }
  }
])
```

### Output

| Fraud Probability Bucket | Count |
|---:|---:|
| 0 to 0.25 | 1,982 |
| 0.50 to 0.75 | 1 |
| 0.75 to 1.00 | 17 |

### Understanding

Most transactions had very low fraud probability.

Only a small number of transactions fell into higher fraud probability ranges.

This again confirms the fraud detection nature of the dataset, where most transactions are normal and only a small number are risky.

---

## Reusable Python Query Script

A reusable Python script was created for running these MongoDB checks from the project code.

File:

```text
src/database/mongodb_queries.py
```

Run command:

```powershell
python src\database\mongodb_queries.py
```

This script prints:

- total prediction records
- total fraud alerts
- latest predictions
- highest fraud probability transactions
- fraud vs non-fraud prediction count
- actual vs predicted comparison
- fraud alerts by transaction type
- fraud alert amount summary
- false negatives
- fraud probability buckets

---

## Business Insights from MongoDB Queries

The MongoDB query outputs provide important project insights:

- The real-time pipeline successfully processed 2,000 transactions.
- 18 transactions were identified as fraud alerts.
- Fraud alerts were found only in `CASH_OUT` and `TRANSFER` transaction types.
- The maximum fraud alert amount was 1,277,212.77.
- No false negatives appeared in this test sample.
- Most transactions had fraud probability near 0.
- High-risk transactions had fraud probability near 1.

---

## How These Queries Help the Final Dashboard

These queries prepare the base for future dashboard cards.

Possible dashboard metrics:

- Total transactions processed
- Total fraud alerts
- Fraud vs non-fraud distribution
- Fraud alerts by transaction type
- Highest fraud probability transactions
- Maximum fraud amount
- Average fraud amount
- Fraud probability bucket distribution
- False negative monitoring

---

## Final Status

MongoDB query verification was completed successfully.

Final test result:

```text
2,000 prediction records stored
18 fraud alerts generated
MongoDB queries verified successfully
```

This confirms that the real-time fraud detection pipeline is saving prediction outputs correctly and can be analyzed through MongoDB queries.