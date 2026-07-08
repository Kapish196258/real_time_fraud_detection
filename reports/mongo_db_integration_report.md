# MongoDB Integration Report

## Purpose

This report explains how MongoDB is used in the real-time fraud detection project.

After predicting fraud, the result should not only be printed in the terminal. It should be stored in a database so that it can be reviewed later.

For this purpose, MongoDB was added.

---

## Why MongoDB Was Used

MongoDB is a NoSQL database that stores data in document format, similar to JSON.

This is useful because Kafka transaction messages are also JSON-like.

MongoDB is suitable for this project because:

- It can store flexible transaction records.
- It works well with real-time data.
- It connects easily with Python using `pymongo`.
- It stores prediction history.
- It stores fraud alerts separately.
- It can support future dashboarding and monitoring.

---

## Main File

```text
src/database/connectiontext
src/database/connection.py
```

This file contains MongoDB connection and storage utility functions.

---

## Package Used

Python package:

```text
pymongo
```

Install command:

```powershell
pip install pymongo
```

Added in `requirements.txt`:

```text
pymongo==4.17.0
```

---

## MongoDB Setup

MongoDB runs using Docker Compose.

Docker Compose file:

```text
docker/docker-compose.yml
```

Containers:

```text
fraud-mongodb
fraud-mongo-express
```

MongoDB local connection:

```text
mongodb://localhost:27017/
```

Mongo Express UI:

```text
http://localhost:8081
```

---

## Database Name

```text
fraud_detection_db
```

---

## Collections Used

```text
prediction_history
fraud_alerts
```

---

## `prediction_history` Collection

This collection stores all processed transactions.

It includes both fraud and non-fraud predictions.

Purpose:

- Store every prediction result.
- Keep original transaction details.
- Store fraud probability.
- Store Kafka partition and offset.
- Store processing timestamp.

This makes the prediction process trackable.

---

## `fraud_alerts` Collection

This collection stores only transactions predicted as fraud.

A record is saved here only when:

```text
prediction = 1
```

Purpose:

- Keep suspicious transactions separate.
- Make fraud monitoring easier.
- Support future dashboard or alert system.

---

## Important Functions in `connection.py`

### `get_mongo_client()`

Creates and returns MongoDB client.

Default URI:

```text
mongodb://localhost:27017/
```

---

### `get_database()`

Returns the project database:

```text
fraud_detection_db
```

---

### `get_prediction_history_collection()`

Returns the collection:

```text
prediction_history
```

---

### `get_fraud_alerts_collection()`

Returns the collection:

```text
fraud_alerts
```

---

### `setup_database_indexes()`

Creates indexes on important fields such as:

```text
transaction_id
prediction
fraud_probability
processed_at
alert_status
created_at
```

Indexes help make searching and sorting faster.

---

### `save_prediction_history()`

Stores every prediction record in MongoDB.

This function is called by the Kafka consumer after prediction.

---

### `save_fraud_alert()`

Stores only fraud prediction records.

This function is called when the model predicts fraud.

---

### `test_connection()`

Checks whether MongoDB is running properly.

Expected output:

```text
MongoDB connection successful.
Database ready: fraud_detection_db
```

---

## Test Command

Run from project root:

```powershell
python src\database\connection.py
```

---

## Error Faced

Error:

```text
ServerSelectionTimeoutError
No connection could be made because the target machine actively refused it
```

Reason:

MongoDB was not running.

Solution:

```powershell
docker compose -f docker/docker-compose.yml up -d
```

Then check:

```powershell
docker ps
```

After confirming that `fraud-mongodb` is running, test again:

```powershell
python src\database\connection.py
```

---

## Current Status

MongoDB integration is working.

Visible in Mongo Express:

```text
Database: fraud_detection_db
Collection: prediction_history
```

This confirms that MongoDB is connected successfully.

---

## Why This Step Is Important

Without MongoDB, prediction results would disappear after the terminal closes.

With MongoDB, the project stores real-time prediction results permanently.

This makes the system more practical and closer to a real-world fraud detection pipeline.

---

## Final Understanding

MongoDB is the storage layer of the project.

Kafka brings transactions, the model predicts fraud, and MongoDB stores the prediction results and fraud alerts.