# Real-Time Financial Fraud Detection Pipeline

An end-to-end machine learning and real-time streaming project built with the PaySim financial transaction dataset, Apache Kafka, Spark Structured Streaming, MongoDB, Prometheus, Python, and Docker.

The system streams financial transactions through Kafka, applies a trained Random Forest model inside Spark micro-batches, stores predictions and fraud alerts in MongoDB, and exposes live operational metrics for monitoring.

## Architecture

```text
PaySim CSV Dataset
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
Random Forest Fraud Prediction
        |
        +----------------------------+
        |                            |
        v                            v
MongoDB                      Prometheus Metrics
        |
        +--> prediction_history
        |
        +--> fraud_alerts
```

## Project Status

| Component | Status |
|---|---|
| Data understanding and validation | Completed |
| Exploratory data analysis | Completed |
| Feature engineering | Completed |
| Model training and evaluation | Completed |
| Random Forest model serialization | Completed |
| Kafka producer and topic setup | Completed |
| Python Kafka fraud consumer | Completed |
| Spark Structured Streaming consumer | Completed |
| Spark fraud-prediction integration | Completed |
| Spark-to-MongoDB persistence | Completed |
| MongoDB batch upserts | Completed |
| Fraud-alert storage | Completed |
| Prometheus application metrics | Completed |
| Prometheus Docker service | Next |
| Grafana dashboard | Pending |
| Larger-volume load testing | Pending |
| Performance optimization | Pending |
| Final deployment documentation | Pending |

## Technology Stack

| Area | Technology |
|---|---|
| Language | Python 3.12 |
| Dataset | PaySim |
| Data processing | pandas 3.0.3, NumPy 2.5.0 |
| Machine learning | scikit-learn 1.9.0 |
| Streaming | Apache Kafka, kafka-python 3.0.7 |
| Stream processing | PySpark 3.5.1 |
| Database | MongoDB 7, PyMongo 4.17.0 |
| Monitoring | prometheus-client 0.25.0 |
| Infrastructure | Docker Compose |
| Development environment | Windows, PowerShell, VS Code |

The complete Python dependency list is maintained in:

```text
requirements.txt
```

## Dataset and Model

The project uses the PaySim synthetic financial transaction dataset.

| Dataset metric | Value |
|---|---:|
| Total transactions | 6,362,620 |
| Fraud transactions | 8,213 |
| Non-fraud transactions | 6,354,407 |
| Fraud rate | 0.1291% |

Fraud occurs only in `TRANSFER` and `CASH_OUT` transactions.

The final selected model is a Random Forest classifier.

| Model metric | Result |
|---|---:|
| Accuracy | 0.999997 |
| Precision | 1.000000 |
| Recall | 0.997565 |
| F1 score | 0.998781 |
| False positives | 0 |
| False negatives | 4 |

Detailed data-analysis and model-evaluation results are available in the reports directory.

## Main Pipeline Components

```text
src/streaming/producer.py
```

Reads PaySim transactions, adds a transaction ID and stream timestamp, converts each record to JSON, and sends it to Kafka.

```text
src/models/predict_fraud.py
```

Loads the trained model and feature list, prepares one transaction, and returns the fraud prediction and probability.

```text
src/streaming/spark_fraud_pipeline.py
```

Consumes Kafka records using Spark Structured Streaming, performs model scoring inside `foreachBatch()`, writes results to MongoDB, and updates Prometheus metrics.

```text
src/database/connection.py
```

Manages MongoDB connections, indexes, batch upserts, prediction-history storage, and fraud-alert storage.

```text
src/monitoring/metrics.py
```

Defines Prometheus counters, gauges, and histograms for pipeline monitoring.

## MongoDB Storage

Database:

```text
fraud_detection_db
```

Collections:

```text
prediction_history
fraud_alerts
```

`prediction_history` stores every successfully scored transaction.

`fraud_alerts` stores transactions where:

```text
prediction = 1
```

Writes use `transaction_id`-based upserts so repeated processing updates existing documents instead of creating repeated test records.

## Prometheus Metrics

The Spark process exposes metrics at:

```text
http://localhost:8000/metrics
```

Available metrics include:

- processed transactions;
- predicted fraud alerts;
- prediction failures;
- completed, empty, and failed Spark batches;
- true positives and true negatives;
- false positives and false negatives;
- MongoDB prediction and alert operations;
- latest batch size;
- latest processing duration;
- latest prediction throughput;
- Spark batch-duration histogram.

Prometheus is currently exposed by the application. The Prometheus Docker service and Grafana dashboard are the next implementation stage.

## Project Structure

```text
real_time_fraud_detection/
├── data/
│   ├── raw/
│   ├── processed/
│   └── predicted/
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
├── src/
│   ├── database/
│   │   └── connection.py
│   ├── features/
│   │   └── feature_engineering.py
│   ├── models/
│   │   ├── predict_fraud.py
│   │   └── train_model.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   └── metrics.py
│   ├── preprocessing/
│   │   └── clean_data.py
│   └── streaming/
│       ├── consumer.py
│       ├── producer.py
│       ├── spark_streaming_consumer.py
│       └── spark_fraud_pipeline.py
├── checkpoints/
├── requirements.txt
└── README.md
```

## Documentation

Detailed implementation evidence is kept in `reports/` so the README remains concise.

| Report | Purpose |
|---|---|
| `model_training_report.md` | Model comparison, evaluation metrics, and final model selection |
| `kafka_streaming_report.md` | Kafka infrastructure, producer, topic, and streaming validation |
| `mongodb_integration_report.md` | Initial MongoDB connection and persistence implementation |
| `mongodb_queries.md` | Queries for validating prediction history and fraud alerts |
| `real_time_prediction_flow.md` | Python Kafka consumer, model prediction, and MongoDB flow |
| `end_to_end_pipeline_testing.md` | Full Python pipeline validation |
| `spark_streaming_consumer_report.md` | Kafka-to-Spark consumer setup and micro-batch testing |
| `spark_fraud_prediction.md` | Random Forest prediction inside the Spark pipeline |
| `spark_mongodb_integration.md` | Spark prediction persistence and fraud-alert validation |

Refer to the relevant report for setup details, test outputs, troubleshooting notes, metrics, and validation evidence.

## Quick Setup

Clone the repository:

```powershell
git clone https://github.com/Kapish196258/real_time_fraud_detection.git
cd real_time_fraud_detection
```

Create and activate the virtual environment:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

Place the PaySim dataset at:

```text
data/raw/paysim_transactions.csv
```

The dataset and generated model files are not stored in Git because of their size.

## Run the Pipeline

Start the Docker services:

```powershell
docker compose -f .\docker\docker-compose.yml up -d
docker ps
```

Current service addresses:

| Service | Address |
|---|---|
| Kafka | `localhost:9092` |
| Kafka UI | `http://localhost:8080` |
| MongoDB | `localhost:27017` |
| Mongo Express | `http://localhost:8081` |

Start the Spark fraud pipeline in Terminal 1:

```powershell
python -m src.streaming.spark_fraud_pipeline --reset-checkpoint
```

Wait until the initial empty Spark batch is complete.

Send transactions from Terminal 2:

```powershell
python .\src\streaming\producer.py --limit 5 --sleep 0.5
```

The five-record command is used as a smoke test. Larger unique transaction ranges should be used for dashboard and load testing.

## Verify the Results

Check Prometheus metrics while Spark is running:

```powershell
Invoke-WebRequest http://localhost:8000/metrics -UseBasicParsing |
Select-Object -ExpandProperty Content |
Select-String "fraud_"
```

Check MongoDB collection counts:

```powershell
docker exec fraud-mongodb mongosh fraud_detection_db --eval "print('Predictions:',db.prediction_history.countDocuments()); print('Alerts:',db.fraud_alerts.countDocuments());"
```

The validated five-record test produced:

| Metric | Result |
|---|---:|
| Transactions processed | 5 |
| Predicted fraud alerts | 2 |
| Prediction failures | 0 |
| True positives | 2 |
| True negatives | 3 |
| False positives | 0 |
| False negatives | 0 |

This is a functional smoke test, not a production performance benchmark.

## Next Milestone

The next stage is to add:

- a Prometheus Docker service;
- Prometheus scrape configuration;
- a Grafana service;
- an automatically provisioned Grafana dashboard;
- larger-volume streaming tests;
- throughput and latency analysis;
- Spark and MongoDB performance optimization.

The dashboard will monitor operational metrics from Prometheus. Detailed transaction and fraud-alert records will continue to be stored in MongoDB.

## Team Workflow

Before starting work:

```powershell
git pull origin main
```

After completing and testing an assigned task:

```powershell
git status
git add .
git commit -m "Describe the completed work"
git pull --rebase origin main
git push origin main
```

Avoid force-pushing to `main`.

## Repository

```text
https://github.com/Kapish196258/real_time_fraud_detection
```