# Real-Time Financial Fraud Detection Pipeline

## Project Overview

This project is a real-time financial fraud detection pipeline built using Python, machine learning, Kafka, MongoDB, and Docker.

The main goal of this project is to detect fraudulent financial transactions using a trained machine learning model and then process transaction data in a real-time streaming flow.

The project starts from data understanding and model training, then moves toward a real-time pipeline where transactions are streamed through Kafka, predicted using the saved model, and stored in MongoDB for tracking and future dashboarding.

---

## Problem Statement

Financial fraud detection is important because fraudulent transactions can create direct financial loss.

In real-world systems, transactions happen continuously. So fraud detection should not only work inside a notebook. It should be able to process incoming transactions in a pipeline.

This project solves the problem by:

- Understanding financial transaction data
- Cleaning and preparing the dataset
- Creating fraud-related features
- Training and comparing machine learning models
- Selecting the best fraud detection model
- Streaming transactions using Kafka
- Predicting fraud using a saved model
- Storing prediction results in MongoDB
- Preparing the project for dashboarding and monitoring

---

## Dataset Used

Dataset:

```text
PaySim Synthetic Financial Transaction Dataset
```

Raw dataset path:

```text
data/raw/paysim_transactions.csv
```

Processed dataset path:

```text
data/processed/processed_transactions.csv
```

Target column:

```text
isFraud
```

Target meaning:

```text
0 = Non-fraud transaction
1 = Fraud transaction
```

Important dataset points:

- Total transactions: 6,362,620
- Fraud cases are very rare
- Dataset is highly imbalanced
- Fraud mainly appears in `TRANSFER` and `CASH_OUT` transaction types

---

## Tech Stack

| Area | Tools Used |
|---|---|
| Programming | Python |
| Data Analysis | Pandas, NumPy |
| Machine Learning | Scikit-learn |
| Model Storage | Joblib |
| Streaming | Apache Kafka |
| Database | MongoDB |
| Containerization | Docker, Docker Compose |
| Notebook Work | Jupyter Notebook |
| Version Control | Git, GitHub |
| Future Dashboard/Monitoring | Grafana, Prometheus |

---

## Current Project Status

Completed so far:

- Project folder structure created
- GitHub repository created
- PaySim dataset added locally
- Data understanding notebook completed
- EDA and data cleaning notebook completed
- Feature engineering notebook completed
- Feature engineering script created
- Processed dataset generated locally
- Docker Compose setup completed
- Kafka setup completed
- MongoDB setup completed
- Kafka topic `transactions` created
- Kafka producer created and tested
- Model training notebook completed
- Model training script created
- Random Forest selected as final model
- Saved model files generated locally
- Fraud prediction helper created
- MongoDB database utilities created
- Kafka consumer created
- End-to-end flow tested successfully
- Reports documentation added

---

## Project Folder Structure

```text
real_time_fraud_detection/
├── data/
│   ├── raw/
│   └── processed/
├── notebooks/
│   ├── 01_data_understanding.ipynb
│   ├── 02_eda_and_data_cleaning.ipynb
│   ├── 03_feature_engineering.ipynb
│   └── 04_model_training.ipynb
├── src/
│   ├── data_ingestion/
│   ├── preprocessing/
│   ├── features/
│   │   └── feature_engineering.py
│   ├── models/
│   │   ├── train_model.py
│   │   └── predict_fraud.py
│   ├── streaming/
│   │   ├── producer.py
│   │   └── consumer.py
│   ├── database/
│   │   └── connection.py
│   └── api/
├── docker/
│   └── docker-compose.yml
├── dashboard/
├── reports/
├── models/
│   └── saved_model/
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Important Files and Their Purpose

### `src/features/feature_engineering.py`

Creates processed model-ready transaction data from the raw PaySim dataset.

It creates fraud-related features such as:

- balance difference features
- balance error features
- amount ratio features
- zero balance indicators
- transaction type encoded columns

---

### `src/models/train_model.py`

Trains and compares machine learning models.

Models trained:

- Logistic Regression
- Decision Tree
- Random Forest
- HistGradientBoosting

Final selected model:

```text
Random Forest
```

---

### `src/models/predict_fraud.py`

Loads the saved Random Forest model and predicts fraud for a single incoming transaction.

It also prepares incoming transaction data into the same feature format used during training.

---

### `src/streaming/producer.py`

Reads transaction rows from the PaySim CSV file and sends them to Kafka topic:

```text
transactions
```

---

### `src/streaming/consumer.py`

Reads transactions from Kafka, sends them to the saved model for prediction, and stores results in MongoDB.

Flow:

```text
Kafka message
    -> consumer.py
        -> predict_fraud.py
            -> saved model prediction
        -> MongoDB prediction_history
        -> MongoDB fraud_alerts
```

---

### `src/database/connection.py`

Handles MongoDB connection and database operations.

It connects Python to MongoDB and provides functions to save:

- prediction history
- fraud alerts

---

## Model Training Summary

Final selected model:

```text
Random Forest
```

Random Forest was selected because it gave the best overall performance.

Final model result:

| Metric | Value |
|---|---:|
| Accuracy | 0.999997 |
| Precision | 1.000000 |
| Recall | 0.997565 |
| F1 Score | 0.998781 |
| False Positives | 0 |
| False Negatives | 4 |

Saved model files:

```text
models/saved_model/fraud_detection_model.pkl
models/saved_model/model_features.pkl
models/saved_model/model_metadata.json
```

Important note:

Large generated files such as datasets and model `.pkl` files may not be pushed to GitHub. Each team member should generate them locally using the project scripts.

---

## Docker Services

Docker Compose is used to run:

- Kafka
- Kafka UI
- MongoDB
- Mongo Express

Docker Compose file:

```text
docker/docker-compose.yml
```

Start services:

```powershell
docker compose -f docker/docker-compose.yml up -d
```

Check running containers:

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

Important links:

```text
Kafka UI      : http://localhost:8080
Mongo Express : http://localhost:8081
MongoDB Port  : localhost:27017
Kafka Port    : localhost:9092
```

---

## Kafka Topic

Kafka topic used:

```text
transactions
```

Check topics:

```powershell
docker exec -it fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

Create topic if missing:

```powershell
docker exec -it fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --topic transactions --partitions 3 --replication-factor 1
```

---

## MongoDB Setup

MongoDB is used to store real-time prediction results.

Database:

```text
fraud_detection_db
```

Collections:

```text
prediction_history
fraud_alerts
```

### `prediction_history`

Stores all transactions processed by the model.

### `fraud_alerts`

Stores only transactions predicted as fraud.

Test MongoDB connection:

```powershell
python src\database\connection.py
```

Expected output:

```text
MongoDB connection successful.
Database ready: fraud_detection_db
```

---

## End-to-End Pipeline Flow

Current working pipeline:

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
Saved Random Forest Model
    ↓
MongoDB prediction_history
    ↓
MongoDB fraud_alerts
```

---

## How to Run the Project

### 1. Activate Virtual Environment

```powershell
.\venv\Scripts\Activate.ps1
```

---

### 2. Start Docker Services

```powershell
docker compose -f docker/docker-compose.yml up -d
```

---

### 3. Check Docker Containers

```powershell
docker ps
```

---

### 4. Test MongoDB Connection

```powershell
python src\database\connection.py
```

---

### 5. Run Kafka Consumer

Open one terminal:

```powershell
python src\streaming\consumer.py --group-id full-test-1 --limit 100 --threshold 0.5
```

---

### 6. Run Kafka Producer

Open another terminal:

```powershell
python src\streaming\producer.py --limit 100 --sleep 0.01
```

---

### 7. Check MongoDB Output

Open:

```text
http://localhost:8081
```

Go to:

```text
fraud_detection_db
```

Check collections:

```text
prediction_history
fraud_alerts
```

Optional MongoDB shell check:

```powershell
docker exec -it fraud-mongodb mongosh fraud_detection_db --eval "db.prediction_history.countDocuments(); db.fraud_alerts.countDocuments(); db.prediction_history.findOne();"
```

---

## Reports Documentation

The `reports/` folder contains project documentation.

Current report files:

```text
reports/model_training_report.md
reports/project_progress_report.md
reports/real_time_prediction_flow.md
reports/mongo_db_integration_report.md
reports/kafka_streaming_report.md
```

Purpose of reports:

- Explain model training
- Explain real-time prediction flow
- Explain MongoDB integration
- Explain Kafka streaming
- Track project progress

---

## GitHub and Version Control

GitHub is used for:

- version control
- team collaboration
- tracking commits
- project submission

Latest major work added:

- fraud prediction helper
- MongoDB database utilities
- Kafka fraud detection consumer
- real-time pipeline documentation

---

## Current Learning Outcome

The project has now moved from offline notebook-based model training to a real-time fraud detection pipeline.

Important learning so far:

- How to understand and clean a large financial transaction dataset
- How to create fraud-related features
- How to train and compare machine learning models
- Why Random Forest was selected
- How to stream data using Kafka
- How to use a saved ML model for real-time prediction
- How to store prediction results in MongoDB
- How Docker helps run Kafka and MongoDB services consistently

---

## Pending Next Steps

Next planned work:

- Add MongoDB query documentation
- Add end-to-end pipeline testing documentation
- Start Spark Structured Streaming integration
- Add monitoring metrics
- Build Grafana dashboard
- Add final dashboard documentation
- Prepare final project report
- Prepare final presentation script

---

## Team Note

Before running the project, each team member should:

1. Pull latest repository:

```powershell
git pull origin main
```

2. Place the PaySim dataset locally:

```text
data/raw/paysim_transactions.csv
```

3. Install dependencies:

```powershell
pip install -r requirements.txt
```

4. Generate processed data if missing:

```powershell
python src\features\feature_engineering.py
```

5. Generate model files if missing:

```powershell
python src\models\train_model.py
```

6. Start Docker services and run the pipeline commands.

---

## Final Summary

This project currently demonstrates a working real-time fraud detection flow using Kafka, a saved Random Forest model, and MongoDB.

The current system can stream transactions, predict fraud, and store prediction results for future analysis and dashboarding.