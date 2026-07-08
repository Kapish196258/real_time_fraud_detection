# Kafka Streaming Report

## Purpose

This report explains the Kafka streaming part of the real-time fraud detection project.

The goal is to process transactions like a real-time system, not only through notebooks.

Kafka was used to simulate real-time financial transaction streaming.

---

## What Is Kafka

Apache Kafka is an event streaming platform.

In simple words, Kafka helps send and receive real-time messages.

In this project:

```text
Producer sends transactions
Kafka stores messages in a topic
Consumer reads transactions
Model predicts fraud
MongoDB stores results
```

---

## Why Kafka Was Used

Financial transactions happen continuously.

A fraud detection system should be able to process transactions as they arrive.

Kafka was used because:

- It supports real-time streaming.
- It can handle large message volume.
- It separates producer and consumer.
- It makes the project closer to a real-world fraud detection system.

---

## Kafka Topic

Topic used:

```text
transactions
```

This topic stores transaction messages sent by the producer.

---

## Main Kafka Files

```text
src/streaming/producer.py
src/streaming/consumer.py
docker/docker-compose.yml
```

---

## Docker Containers Used

Kafka container:

```text
fraud-kafka
```

Kafka UI container:

```text
fraud-kafka-ui
```

Kafka UI link:

```text
http://localhost:8080
```

---

## Kafka Ports

For local Python code:

```text
localhost:9092
```

For Docker internal communication:

```text
kafka:29092
```

---

## Start Kafka Services

Run:

```powershell
docker compose -f docker/docker-compose.yml up -d
```

Check containers:

```powershell
docker ps
```

Expected important containers:

```text
fraud-kafka
fraud-kafka-ui
fraud-mongodb
fraud-mongo-express
```

---

## Create Kafka Topic

Create topic:

```powershell
docker exec -it fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --topic transactions --partitions 3 --replication-factor 1
```

Check topic:

```powershell
docker exec -it fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

Expected:

```text
transactions
```

---

## Producer Flow

The producer reads the PaySim dataset and sends rows to Kafka.

Dataset path:

```text
data/raw/paysim_transactions.csv
```

The producer adds:

```text
transaction_id
stream_timestamp
```

Then each transaction is sent as JSON to the Kafka topic:

```text
transactions
```

---

## Producer Command

```powershell
python src\streaming\producer.py --limit 100 --sleep 0.01
```

Expected output:

```text
Streaming completed. Total transactions sent: 100
```

---

## Consumer Flow

The consumer reads messages from Kafka and processes them.

Flow:

```text
Read Kafka message
    ↓
Decode JSON transaction
    ↓
Send transaction to predict_fraud.py
    ↓
Get fraud prediction and probability
    ↓
Save result in MongoDB prediction_history
    ↓
If fraud, save alert in fraud_alerts
    ↓
Print progress or alert in terminal
```

---

## Consumer File

```text
src/streaming/consumer.py
```

---

## Why Consumer Was Created

The producer only sends data to Kafka.

The consumer is needed to:

- Read streamed transactions.
- Call the trained fraud detection model.
- Get prediction output.
- Save results in MongoDB.
- Create fraud alerts.

This completes the real-time flow.

---

## Consumer Command

Terminal 1:

```powershell
python src\streaming\consumer.py --group-id fraud-test-1 --limit 100
```

Terminal 2:

```powershell
python src\streaming\producer.py --limit 100 --sleep 0.01
```

---

## Expected Consumer Output

```text
Starting fraud detection consumer...
Kafka topic      : transactions
Bootstrap server : localhost:9092
Consumer group   : fraud-test-1
Alert threshold  : 0.5
```

If fraud is detected:

```text
FRAUD ALERT | transaction_id=... | amount=... | probability=...
```

At the end:

```text
Consumer stopped.
Total processed: 100
Total alerts   : ...
```

---

## MongoDB Output

Open Mongo Express:

```text
http://localhost:8081
```

Check:

```text
Database: fraud_detection_db
Collections: prediction_history, fraud_alerts
```

Optional verification command:

```powershell
docker exec -it fraud-mongodb mongosh fraud_detection_db --eval "db.prediction_history.countDocuments(); db.fraud_alerts.countDocuments(); db.fraud_alerts.findOne()"
```

---

## Common Issues and Fixes

### Kafka Not Running

```powershell
docker compose -f docker/docker-compose.yml up -d
```

### Topic Not Created

```powershell
docker exec -it fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --topic transactions --partitions 3 --replication-factor 1
```

### Dataset Missing

Dataset should be available at:

```text
data/raw/paysim_transactions.csv
```

### Consumer Not Receiving Messages

Use a new consumer group ID:

```powershell
python src\streaming\consumer.py --group-id fraud-test-2 --limit 100
```

---

## Current Status

Completed:

- Kafka Docker setup created.
- Kafka UI available.
- Topic `transactions` created.
- Producer created and tested.
- Consumer created.
- Consumer connected with prediction model.
- Consumer connected with MongoDB.

---

## Final Pipeline Flow

```text
PaySim CSV
    ↓
Kafka Producer
    ↓
Kafka Topic: transactions
    ↓
Kafka Consumer
    ↓
predict_fraud.py
    ↓
Random Forest prediction
    ↓
MongoDB prediction_history
    ↓
MongoDB fraud_alerts
```

---

## Final Understanding

Kafka is the streaming layer of the project.

It moves transaction data from producer to consumer in real time.

The consumer connects Kafka data with the saved machine learning model and MongoDB storage.