# Real-Time Financial Fraud Detection Pipeline

An end-to-end data analytics, machine learning and real-time streaming project built using the PaySim financial transaction dataset, Python, Apache Kafka, PySpark, MongoDB and Docker.

The project analyzes more than 6.3 million financial transactions, trains a fraud detection model, simulates live transactions through Kafka, performs real-time fraud prediction, stores prediction history and fraud alerts, and processes Kafka events using Apache Spark Structured Streaming.

---

## 1. Project Overview

Financial fraud detection is a highly imbalanced classification problem because fraudulent transactions represent only a very small percentage of total financial activity.

The main objectives of this project are:

- Analyze historical financial transaction data.
- Understand fraud behaviour and transaction patterns.
- Minimize false negatives.
- Train and compare machine learning models.
- Select and serialize the best-performing model.
- Simulate real-time financial transactions using Kafka.
- Perform online fraud prediction.
- Store predictions and fraud alerts in MongoDB.
- Process Kafka transactions using Spark Structured Streaming.
- Build monitoring and dashboard layers.
- Test performance, throughput and latency.

---

## 2. Project Status

| Project Stage | Status |
|---|---|
| Repository and folder setup | Completed |
| Dataset collection | Completed |
| Data understanding | Completed |
| Exploratory data analysis | Completed |
| Data cleaning and validation | Completed |
| Docker infrastructure | Completed |
| Kafka topic setup | Completed |
| Kafka producer | Completed |
| Feature engineering | Completed |
| Model training and comparison | Completed |
| Final model selection | Completed |
| Model serialization | Completed |
| Real-time prediction helper | Completed |
| MongoDB integration | Completed |
| Python Kafka fraud consumer | Completed |
| Prediction history storage | Completed |
| Fraud alert storage | Completed |
| MongoDB query validation | Completed |
| Real-time prediction flow | Completed |
| End-to-end pipeline testing | Completed |
| Spark Structured Streaming consumer | Completed |
| Spark model prediction integration | Next |
| Spark-to-MongoDB integration | Next |
| Monitoring and dashboard | Pending |
| Performance optimization | Pending |
| Load testing | Pending |
| Final architecture documentation | Pending |
| Final presentation and evaluation guide | Pending |

---

## 3. Business Problem

Financial institutions and digital payment platforms process a large number of transactions every second.

Fraudulent transactions are rare but expensive. Missing even a small number of fraud cases can result in:

- Financial loss.
- Customer trust issues.
- Compliance problems.
- Operational risk.
- Reputation damage.

The project therefore focuses heavily on reducing false negatives while maintaining strong precision and system stability.

---

## 4. Dataset

The project uses the PaySim synthetic financial transaction dataset.

### Dataset Source

```text
PaySim Synthetic Financial Dataset
```

### Local Dataset Path

```text
data/raw/paysim_transactions.csv
```

### Dataset Summary

| Metric | Value |
|---|---:|
| Total transactions | 6,362,620 |
| Total raw columns | 11 |
| Non-fraud transactions | 6,354,407 |
| Fraud transactions | 8,213 |
| Fraud rate | 0.1291% |
| `isFlaggedFraud = 1` | 16 |
| Actual fraud missed by the original rule | 8,197 |

### Raw Dataset Columns

```text
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
```

### Fraud by Transaction Type

Fraudulent transactions occur only in:

```text
TRANSFER
CASH_OUT
```

Fraud does not occur in:

```text
PAYMENT
CASH_IN
DEBIT
```

---

## 5. Technology Stack

### Programming and Data Analysis

- Python
- pandas
- NumPy
- Jupyter Notebook
- matplotlib
- seaborn

### Machine Learning

- scikit-learn
- Logistic Regression
- Decision Tree
- Random Forest
- HistGradientBoosting
- Model serialization using pickle or joblib

### Streaming

- Apache Kafka
- kafka-python
- Apache Spark
- PySpark
- Spark Structured Streaming
- Spark SQL Kafka connector

### Database

- MongoDB
- PyMongo
- Mongo Express

### Infrastructure

- Docker Desktop
- Docker Compose
- Kafka UI
- PowerShell
- VS Code
- Windows 11

### Planned Monitoring

- Grafana
- Prometheus
- Throughput metrics
- Latency metrics
- Kafka lag
- Batch-processing metrics

---

## 6. System Architecture

```text
PaySim CSV Dataset
        |
        v
Python Kafka Producer
src/streaming/producer.py
        |
        v
Kafka Topic: transactions
        |
        +----------------------------------+
        |                                  |
        v                                  v
Python Fraud Consumer              Spark Structured Streaming
src/streaming/consumer.py          spark_streaming_consumer.py
        |                                  |
        v                                  v
Fraud Prediction Helper            Parsed Spark Micro-Batches
src/models/predict_fraud.py                |
        |                                  +--> Next: Model Scoring
        v                                  +--> Next: MongoDB Storage
MongoDB                                  +--> Next: Metrics
        |
        +--> prediction_history
        |
        +--> fraud_alerts
        |
        v
Mongo Express / Grafana / Monitoring
```

---

## 7. Repository Structure

```text
real_time_fraud_detection/
├── dashboard/
├── data/
│   ├── predicted/
│   │   ├── fraud_alerts.csv
│   │   └── prediction_history.csv
│   ├── processed/
│   └── raw/
├── docker/
│   └── docker-compose.yml
├── models/
│   └── saved_model/
├── notebooks/
│   ├── 01_data_understanding.ipynb
│   ├── 02_eda_and_data_cleaning.ipynb
│   ├── 03_feature_engineering.ipynb
│   └── 04_model_training.ipynb
├── reports/
│   ├── end_to_end_pipeline_testing.md
│   ├── kafka_streaming_report.md
│   ├── model_training_report.md
│   ├── mongodb_integration_report.md
│   ├── mongodb_queries.md
│   ├── real_time_prediction_flow.md
│   └── spark_streaming_consumer_report.md
├── src/
│   ├── api/
│   ├── data_ingestion/
│   │   └── load_data.py
│   ├── database/
│   │   └── connection.py
│   ├── features/
│   │   └── feature_engineering.py
│   ├── models/
│   │   ├── predict_fraud.py
│   │   └── train_model.py
│   ├── preprocessing/
│   │   └── clean_data.py
│   └── streaming/
│       ├── consumer.py
│       ├── producer.py
│       └── spark_streaming_consumer.py
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 8. Data Understanding

The first notebook is:

```text
notebooks/01_data_understanding.ipynb
```

This stage includes:

- Loading the PaySim dataset.
- Checking dataset dimensions.
- Reviewing column names.
- Inspecting data types.
- Calculating fraud counts.
- Calculating fraud rate.
- Reviewing transaction type distribution.
- Comparing fraud and non-fraud transactions.
- Analyzing `isFlaggedFraud`.
- Confirming severe class imbalance.

The dataset contains more than 6.3 million rows and requires memory-efficient processing.

---

## 9. Exploratory Data Analysis and Cleaning

Notebook:

```text
notebooks/02_eda_and_data_cleaning.ipynb
```

Completed checks include:

- Missing-value validation.
- Duplicate checks.
- Transaction-type validation.
- Fraud-count validation.
- Fraud-rate calculation.
- Fraud analysis by transaction type.
- Dataset consistency checks.
- Chunk-based processing.

### Cleaning Decision

No major missing-value or duplicate issue required removing a large number of records.

The validated raw dataset was therefore considered suitable for feature engineering.

---

## 10. Docker, Kafka and MongoDB Setup

The project infrastructure is defined in:

```text
docker/docker-compose.yml
```

The Docker environment includes:

- Apache Kafka
- Kafka UI
- MongoDB
- Mongo Express

### Start Docker Services

```powershell
docker compose -f .\docker\docker-compose.yml up -d
```

### Check Running Containers

```powershell
docker ps
```

### Service Addresses

| Service | Address |
|---|---|
| Kafka from Windows | `localhost:9092` |
| Kafka inside Docker | `kafka:29092` |
| Kafka UI | `http://localhost:8080` |
| Mongo Express | `http://localhost:8081` |

---

## 11. Kafka Topic Setup

The main Kafka topic is:

```text
transactions
```

### Create Topic

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --topic transactions --partitions 3 --replication-factor 1
```

### List Topics

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

### Describe Topic

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic transactions
```

### Kafka Topic Configuration

| Setting | Value |
|---|---|
| Topic | `transactions` |
| Partitions | 3 |
| Replication factor | 1 |

---

## 12. Kafka Producer

File:

```text
src/streaming/producer.py
```

The Kafka producer:

- Reads `paysim_transactions.csv` in chunks.
- Adds `transaction_id`.
- Adds `stream_timestamp`.
- Converts transactions into JSON.
- Sends messages to Kafka.
- Supports configurable transaction limit.
- Supports configurable delay.

### Producer Test Command

```powershell
python .\src\streaming\producer.py --limit 20 --sleep 0.5
```

### Producer Output

```text
Dataset path     : data\raw\paysim_transactions.csv
Kafka topic      : transactions
Bootstrap server : localhost:9092
Chunk size       : 50000
Limit            : 20
Sleep time       : 0.5 seconds

Streaming completed. Total transactions sent: 20
```

---

## 13. Feature Engineering

Notebook:

```text
notebooks/03_feature_engineering.ipynb
```

Reusable script:

```text
src/features/feature_engineering.py
```

### Engineered Features

```text
transaction_id
is_origin_customer
is_dest_customer
is_dest_merchant
origin_balance_diff
destination_balance_diff
origin_balance_error
destination_balance_error
abs_origin_balance_error
abs_destination_balance_error
amount_to_oldbalanceOrg_ratio
amount_to_oldbalanceDest_ratio
is_zero_oldbalanceOrg
is_zero_oldbalanceDest
type_CASH_IN
type_CASH_OUT
type_DEBIT
type_PAYMENT
type_TRANSFER
```

High-cardinality identifiers such as `nameOrig` and `nameDest` are not used directly for model training.

### Processed Dataset

```text
data/processed/processed_transactions.csv
```

### Processed Shape

```text
6,362,620 rows × 28 columns
```

### Final Model Feature Count

```text
23
```

### Run Feature Engineering

```powershell
python .\src\features\feature_engineering.py
```

---

## 14. Model Training

Notebook:

```text
notebooks/04_model_training.ipynb
```

Reusable script:

```text
src/models/train_model.py
```

### Models Compared

- Logistic Regression
- Decision Tree
- Random Forest
- HistGradientBoosting

### Model Selection Criteria

The final model was selected using:

1. Lowest false negatives.
2. Highest recall.
3. Strong F1 score.
4. High precision.
5. Stable real-time suitability.

### Final Selected Model

```text
Random Forest
```

### Final Model Results

| Metric | Result |
|---|---:|
| Accuracy | 0.999997 |
| Precision | 1.000000 |
| Recall | 0.997565 |
| F1 score | 0.998781 |
| False positives | 0 |
| False negatives | 4 |
| True positives | 1,639 |
| True negatives | 1,270,881 |

### Run Model Training

```powershell
python .\src\models\train_model.py
```

### Generated Model Files

```text
models/saved_model/fraud_detection_model.pkl
models/saved_model/model_features.pkl
models/saved_model/model_metadata.json
```

These generated files are local and are not normally pushed to GitHub.

---

## 15. Fraud Prediction Helper

File:

```text
src/models/predict_fraud.py
```

The prediction helper:

- Loads the Random Forest model.
- Loads the saved feature list.
- Prepares one transaction for prediction.
- Maintains the same feature order used during training.
- Generates fraud prediction.
- Generates fraud probability.
- Returns model metadata.

### Run Prediction Test

```powershell
python .\src\models\predict_fraud.py
```

---

## 16. MongoDB Integration

File:

```text
src/database/connection.py
```

### MongoDB Database

```text
fraud_detection_db
```

### Collections

```text
prediction_history
fraud_alerts
```

### Database Responsibilities

- Connect Python to MongoDB.
- Store every fraud prediction.
- Store predicted fraud alerts separately.
- Support MongoDB queries.
- Support dashboard integration.
- Support historical prediction analysis.

### Test Database Connection

```powershell
python .\src\database\connection.py
```

### Check MongoDB Counts

```powershell
docker exec fraud-mongodb mongosh fraud_detection_db --eval "db.prediction_history.countDocuments(); db.fraud_alerts.countDocuments();"
```

### Check One Fraud Alert

```powershell
docker exec fraud-mongodb mongosh fraud_detection_db --eval "db.fraud_alerts.findOne()"
```

---

## 17. Python Kafka Fraud Consumer

File:

```text
src/streaming/consumer.py
```

The Python fraud consumer:

- Reads transactions from Kafka.
- Applies the prediction helper.
- Stores all predictions.
- Stores predicted fraud alerts.
- Supports Kafka consumer group IDs.
- Supports configurable message limits.
- Supports real-time prediction testing.

### Terminal 1 — Consumer

```powershell
python .\src\streaming\consumer.py --group-id final-test-1 --limit 100
```

### Terminal 2 — Producer

```powershell
python .\src\streaming\producer.py --limit 100 --sleep 0.01
```

---

## 18. Real-Time Prediction Flow

The completed Python real-time prediction flow is:

```text
PaySim Dataset
      |
      v
Kafka Producer
      |
      v
Kafka Topic
      |
      v
Python Fraud Consumer
      |
      v
Prediction Helper
      |
      +--> Prediction History
      |
      +--> Fraud Alerts
      |
      v
MongoDB
```

This flow proves that the model can be used outside the training notebook.

---

## 19. End-to-End Pipeline Testing

The complete Python-based pipeline was tested using:

1. Docker services.
2. Kafka topic.
3. Kafka producer.
4. Fraud prediction consumer.
5. Random Forest prediction helper.
6. MongoDB prediction storage.
7. MongoDB alert storage.
8. MongoDB query validation.

The test confirmed that live transactions could move through the complete system.

---

## 20. Spark Structured Streaming Consumer

File:

```text
src/streaming/spark_streaming_consumer.py
```

The Spark consumer:

- Creates a Spark session.
- Loads the Spark Kafka connector.
- Connects to Kafka.
- Subscribes to the `transactions` topic.
- Reads live Kafka messages.
- Converts Kafka values from binary to string.
- Parses JSON using a Spark schema.
- Selects important transaction fields.
- Processes records in micro-batches.
- Displays output in the console.
- Uses checkpointing.
- Supports clean local testing.
- Handles manual shutdown.

### Run Spark Consumer

```powershell
python .\src\streaming\spark_streaming_consumer.py
```

### Run Producer in Another Terminal

```powershell
python .\src\streaming\producer.py --limit 20 --sleep 0.5
```

---

## 21. Spark Output Columns

| Column | Meaning |
|---|---|
| `transaction_id` | Producer-generated sequential ID |
| `step` | PaySim simulation step |
| `type` | Transaction type |
| `amount` | Transaction amount |
| `nameOrig` | Origin account |
| `nameDest` | Destination account |
| `isFraud` | Original PaySim fraud label |
| `stream_timestamp` | Time at which the producer sent the event |

---

## 22. Spark Test Results

### Producer Result

```text
Streaming completed. Total transactions sent: 20
```

### Spark Batch Summary

| Spark Batch | Record Count | Transaction IDs |
|---:|---:|---|
| Batch 0 | 0 | No records |
| Batch 1 | 1 | 1 |
| Batch 2 | 10 | Records received from Kafka partitions |
| Batch 3 | 3 | 12, 13, 14 |
| Batch 4 | 3 | 15, 16, 17 |
| Batch 5 | 2 | 18, 19 |
| Batch 6 | 1 | 20 |
| **Total** | **20** | **1–20** |

`Batch 0` was empty because the Spark consumer started before the producer sent records.

### Representative Spark Output

| Transaction ID | Type | Amount | Fraud Label |
|---:|---|---:|---:|
| 1 | PAYMENT | 9839.64 | 0 |
| 2 | PAYMENT | 1864.28 | 0 |
| 3 | TRANSFER | 181.00 | 1 |
| 4 | CASH_OUT | 181.00 | 1 |
| 5 | PAYMENT | 11668.14 | 0 |
| 10 | DEBIT | 5337.77 | 0 |
| 12 | PAYMENT | 3099.97 | 0 |
| 15 | PAYMENT | 4098.78 | 0 |
| 16 | CASH_OUT | 229133.94 | 0 |
| 18 | PAYMENT | 1157.86 | 0 |
| 19 | PAYMENT | 671.64 | 0 |
| 20 | TRANSFER | 215310.30 | 0 |

### Fraud Records Visible

| Transaction ID | Type | Amount | `isFraud` |
|---:|---|---:|---:|
| 3 | TRANSFER | 181.00 | 1 |
| 4 | CASH_OUT | 181.00 | 1 |

The test confirmed that Spark correctly parsed both normal and fraud-labelled events.

---

## 23. Spark Checkpoint Handling

The checkpoint directory is:

```text
checkpoints/spark_streaming_consumer
```

The Spark query uses:

```python
.option("checkpointLocation", str(CHECKPOINT_DIR))
```

The script also removes old checkpoint data before a fresh local test.

The checkpoint folder should not be committed.

Recommended `.gitignore` entry:

```gitignore
checkpoints/
```

---

## 24. Windows Hadoop Configuration

The initial Spark test failed with:

```text
NativeIO$Windows.access0
```

The issue was related to local Windows Hadoop filesystem access.

The fix required a local Hadoop folder containing:

```text
winutils.exe
hadoop.dll
```

After configuring the Hadoop environment, Spark successfully:

- started;
- connected to Kafka;
- created checkpoints;
- processed micro-batches;
- displayed the 20 streamed records.

The Hadoop binaries are local setup dependencies and should not be committed.

---

## 25. Reports

The `reports` folder contains milestone documentation for:

- model training;
- Kafka streaming;
- MongoDB integration;
- MongoDB queries;
- real-time prediction flow;
- end-to-end pipeline testing;
- Spark Structured Streaming.

The Spark report is:

```text
reports/spark_streaming_consumer_report.md
```

---

## 26. Team Workflow

This is a team project.

Each team member should:

1. Work on their assigned branch.
2. Pull the latest main branch before starting.
3. Commit only assigned files.
4. Use clear commit messages.
5. Push to their own branch.
6. Share the commit hash.
7. Request review before merging.
8. Avoid editing the same file simultaneously.
9. Keep the work-distribution document updated.
10. Maintain regular GitHub activity.

### Kapish's Work

- Spark Structured Streaming consumer.
- Kafka-to-Spark integration.
- Spark schema parsing.
- Checkpoint management.
- Windows Hadoop troubleshooting.
- Kafka topic reset.
- Clean 20-message Spark test.
- Spark output validation.
- Spark report preparation.

### Teammate's Work

- Pull the latest project code.
- Reproduce the Spark environment.
- Run the Spark consumer locally.
- Verify the 20-message test.
- Update the complete `README.md`.
- Commit the README from the teammate branch.
- Share the output screenshot and commit hash.

---

## 27. Installation

### Clone Repository

```powershell
git clone https://github.com/Kapish196258/real_time_fraud_detection.git
cd real_time_fraud_detection
```

### Create Virtual Environment

```powershell
python -m venv venv
```

### Activate Virtual Environment

```powershell
.\venv\Scripts\Activate.ps1
```

If PowerShell blocks activation:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Install Dependencies

```powershell
python -m pip install --upgrade pip
pip install -r .\requirements.txt
```

---

## 28. Dataset Setup

Download the PaySim dataset.

Rename the CSV file to:

```text
paysim_transactions.csv
```

Place it in:

```text
data/raw/paysim_transactions.csv
```

The dataset is large and should not be committed.

---

## 29. Generate Local Processed Data and Models

### Feature Engineering

```powershell
python .\src\features\feature_engineering.py
```

### Model Training

```powershell
python .\src\models\train_model.py
```

These commands generate:

```text
data/processed/processed_transactions.csv
models/saved_model/fraud_detection_model.pkl
models/saved_model/model_features.pkl
models/saved_model/model_metadata.json
```

---

## 30. Recommended Spark Demonstration

### Terminal 1 — Start Docker

```powershell
docker compose -f .\docker\docker-compose.yml up -d
docker ps
```

### Terminal 2 — Start Spark Consumer

```powershell
.\venv\Scripts\Activate.ps1
python .\src\streaming\spark_streaming_consumer.py
```

### Terminal 3 — Start Producer

```powershell
.\venv\Scripts\Activate.ps1
python .\src\streaming\producer.py --limit 20 --sleep 0.5
```

The Spark terminal should display transaction IDs 1–20 across multiple micro-batches.

---

## 31. Next Development Stage

The next major task is Spark-based fraud scoring.

Recommended new file:

```text
src/streaming/spark_fraud_pipeline.py
```

The next pipeline should:

1. Read Kafka records.
2. Parse JSON.
3. Use `foreachBatch()`.
4. Prepare the 23 trained model features.
5. Load the Random Forest model.
6. Generate fraud prediction.
7. Generate fraud probability.
8. Add batch ID.
9. Add prediction timestamp.
10. Store all predictions in MongoDB.
11. Store predicted fraud alerts separately.
12. Record processing time and throughput.

---

## 32. Monitoring and Dashboard

The final project should display:

- Total processed transactions.
- Transactions per second.
- Fraud alerts.
- Fraud rate.
- Fraud by transaction type.
- Fraud probability distribution.
- Batch processing duration.
- End-to-end latency.
- Kafka consumer lag.
- Service status.

Grafana and Prometheus can be used for operational monitoring.

---

## 33. Load Testing

The pipeline should be tested using:

```text
20 transactions
100 transactions
1,000 transactions
10,000 transactions
Continuous transaction stream
```

For every test, record:

- Transactions produced.
- Transactions consumed.
- Predictions stored.
- Fraud alerts stored.
- Processing time.
- Throughput.
- Average latency.
- Maximum latency.
- Failed records.
- Database counts.

---

## 34. Performance Optimization

Test and optimize:

- Kafka partition count.
- Spark trigger interval.
- Maximum offsets per trigger.
- Spark shuffle partitions.
- Spark driver memory.
- MongoDB bulk-write size.
- MongoDB indexes.
- Producer delay.
- Batch-processing time.

---

## 35. Known Limitations

- PaySim is synthetic.
- The Random Forest model is trained offline.
- The current Spark consumer parses and displays records but does not yet apply the model.
- Windows Spark requires Hadoop compatibility configuration.
- The current deployment is local.
- Authentication and production security are not yet implemented.
- Kafka schema registry is not currently used.
- Dead-letter topic handling is not yet implemented.
- Model drift monitoring is not yet implemented.

---

## 36. Future Enhancements

- Spark model scoring.
- Spark-to-MongoDB writes.
- Kafka fraud-alert output topic.
- Dead-letter topic.
- Schema registry.
- Grafana dashboard.
- Prometheus metrics.
- Model drift detection.
- Automated model retraining.
- CI/CD pipeline.
- Dockerized Spark service.
- Structured logging.
- Email or notification alerts.
- Production security.
- Cloud deployment.

---

## 37. Recommended `.gitignore`

```gitignore
venv/
.venv/
__pycache__/
*.pyc
.env
.ipynb_checkpoints/
data/raw/*.csv
data/processed/*.csv
models/saved_model/*.pkl
checkpoints/
*.log
.DS_Store
```

---

## 38. Repository

```text
https://github.com/Kapish196258/real_time_fraud_detection
```

---

## 39. Conclusion

The project has progressed from historical fraud analysis to a working real-time financial fraud detection pipeline.

Completed components include:

- PaySim data understanding.
- EDA and validation.
- Feature engineering.
- Random Forest model training.
- Model serialization.
- Docker infrastructure.
- Kafka transaction producer.
- Python fraud prediction consumer.
- MongoDB prediction storage.
- MongoDB fraud alert storage.
- End-to-end pipeline testing.
- Spark Structured Streaming Kafka consumer.
- Windows Hadoop compatibility.
- Spark checkpoint handling.
- Successful 20-message Spark test.

The next major goal is to apply the trained Random Forest model inside Spark micro-batches, store Spark-generated predictions and fraud alerts in MongoDB, and complete monitoring, dashboarding, optimization and load testing.