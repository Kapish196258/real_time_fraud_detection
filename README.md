# Real-Time Financial Fraud Detection Pipeline

 The goal of this project is to build a real-time financial fraud detection pipeline that can process transaction data, identify suspicious activity, and support fraud monitoring using data engineering and machine learning concepts.

At this stage, the project structure has been initialized and the repository is being prepared for regular development, commits, and documentation.

---

## Project Goal

The main goal of this project is to detect potentially fraudulent financial transactions using a structured data pipeline.

The project will include:

* Data ingestion
* Data cleaning and preprocessing
* Exploratory data analysis
* Feature engineering
* Fraud detection model training
* Real-time transaction simulation
* Streaming-based processing
* Fraud alert storage
* Monitoring and dashboarding
* Final documentation

---

## Planned Dataset

The project will use the PaySim Synthetic Dataset for Mobile Money Transactions.

This dataset contains simulated financial transactions and is commonly used for fraud detection projects. It includes transaction types, transaction amounts, account balances, and fraud labels.

The dataset has not been added to this repository yet.

---

## Planned Tech Stack

* Python
* Pandas
* NumPy
* Scikit-learn
* Apache Kafka
* Apache Spark / PySpark
* MongoDB
* Grafana
* Prometheus
* Docker
* Git and GitHub

---

## Current Project Status

The project has been initialized with a clean folder structure.

Current progress:

- Repository initialized with a structured project layout
- Initial dataset understanding completed
- Exploratory Data Analysis (EDA) completed
- Data cleaning and validation completed
- Docker infrastructure configured
- Apache Kafka configured using Docker Compose
- Kafka UI configured for topic monitoring
- MongoDB configured for storing processed transactions
- Mongo Express configured for database management
- Kafka topic `transactions` created with **3 partitions**
- Development environment synchronized using Git


---

## Docker Infrastructure

Start all required services using Docker Compose:

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Available Services

| Service | URL |
|----------|-----|
| Kafka UI | http://localhost:8080 |
| Mongo Express | http://localhost:8081 |

Ensure Docker Desktop is running before executing the above command.

## Current Data Pipeline

```text
PaySim Dataset
       │
       ▼
Python Kafka Producer
       │
       ▼
Kafka Topic (transactions)
       │
       ▼
Kafka Consumer
       │
       ▼
MongoDB
       │
       ▼
Fraud Detection Pipeline
```

## Project Structure

```text
real_time_fraud_detection/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── docker/
│    └── docker-compose.yml
│
├── notebooks/
│
├── src/
│   ├── data_ingestion/
│   ├── preprocessing/
│   ├── features/
│   ├── models/
│   ├── streaming/
│   │   └── producer.py
│   ├── database/
│   └── api/
│
├── dashboard/
│
├── reports/
│
├── models/
│   └── saved_model/
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Folder Purpose

* `data/raw`: stores the original dataset after download
* `data/processed`: stores cleaned and processed data
* `notebooks`: contains analysis and experimentation notebooks
* `src`: contains reusable Python source code
* `src/data_ingestion`: scripts for loading data
* `src/preprocessing`: scripts for cleaning and preparing data
* `src/features`: feature engineering logic
* `src/models`: model training and prediction scripts
* `src/streaming`: Kafka and streaming-related scripts
* `src/database`: database connection and logging logic
* `src/api`: API-related files
* `dashboard`: dashboard files, screenshots, and monitoring notes
* `reports`: final project documentation and reports
* `models/saved_model`: saved trained model files

---

## Next Steps

The upcoming development tasks include:

1. Implement the Python Kafka Producer (`src/streaming/producer.py`)
2. Stream PaySim transactions into the Kafka `transactions` topic
3. Perform feature engineering for fraud detection
4. Train and evaluate machine learning models
5. Build the Kafka consumer to process streaming transactions
6. Store processed transactions in MongoDB
7. Develop REST APIs using FastAPI
8. Build dashboards for fraud monitoring and visualization
9. Complete testing, documentation, and deployment

---