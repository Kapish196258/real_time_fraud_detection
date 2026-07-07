# Real-Time Financial Fraud Detection Pipeline

The goal of this project is to build a real-time financial fraud detection pipeline that can process transaction data, identify suspicious activity, and support fraud monitoring using data engineering and machine learning concepts.

This project combines data engineering, machine learning, and real-time streaming technologies to simulate and detect fraudulent financial transactions using the PaySim dataset.

---

# Project Goal

The main goal of this project is to detect potentially fraudulent financial transactions using a structured data pipeline.

The project includes:

- Data ingestion
- Data cleaning and preprocessing
- Exploratory Data Analysis (EDA)
- Feature engineering
- Fraud detection model training
- Real-time transaction simulation
- Streaming-based processing
- Fraud alert storage
- Monitoring and dashboarding
- Final documentation

---

# Dataset

The project uses the **PaySim Synthetic Dataset for Mobile Money Transactions**.

The dataset contains simulated financial transactions and includes:

- Transaction type
- Transaction amount
- Account balances
- Fraud labels (`isFraud`)
- Flagged fraud transactions

> **Note:** The raw dataset is not stored in this repository due to its large size.

---

# Tech Stack

- Python
- Pandas
- NumPy
- Scikit-learn
- Apache Kafka
- MongoDB
- Mongo Express
- Kafka UI
- Docker & Docker Compose
- Git & GitHub

---

# Current Project Status

Current progress includes:

- Repository initialized with a structured project layout
- Initial dataset understanding completed
- Exploratory Data Analysis (EDA) completed
- Data cleaning and validation completed
- Feature engineering completed
- Docker infrastructure configured
- Apache Kafka configured using Docker Compose
- Kafka UI configured for topic monitoring
- MongoDB configured for storing processed transactions
- Mongo Express configured for database management
- Kafka topic `transactions` created with **3 partitions**
- Kafka producer implemented for transaction streaming
- Development environment synchronized using Git

---

# Docker Infrastructure

Start all required services:

```bash
docker compose -f docker/docker-compose.yml up -d
```

## Available Services

| Service | URL |
|----------|-----|
| Kafka UI | http://localhost:8080 |
| Mongo Express | http://localhost:8081 |

Ensure Docker Desktop is running before starting the containers.

---

# Current Data Pipeline

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

---

# Current Project Progress

The project has successfully completed the offline data preparation and model training stages.

## Completed Work

- Project folder structure created
- PaySim dataset added locally inside `data/raw/`
- Data understanding notebook completed
- Exploratory Data Analysis (EDA) completed
- Data cleaning and validation notebook completed
- Feature engineering notebook completed
- Processed dataset generated locally
- Docker Compose setup completed for Kafka, Kafka UI, MongoDB, and Mongo Express
- Kafka topic `transactions` created
- Python Kafka producer created for streaming PaySim transactions
- Model training notebook completed
- Four machine learning models trained and compared:
  - Logistic Regression
  - Decision Tree
  - Random Forest
  - HistGradientBoostingClassifier
- Final model selected: **Random Forest**
- Model training pipeline script added:
  - `src/models/train_model.py`

---

# Final Model Result

The final selected model is **Random Forest** because it provides the best balance between precision, recall, F1-score, false positives, and false negatives.

| Metric | Value |
|--------|-------:|
| Accuracy | 0.999997 |
| Precision | 1.000000 |
| Recall | 0.997565 |
| F1-Score | 0.998781 |
| False Positives | 0 |
| False Negatives | 4 |
| True Positives | 1639 |
| True Negatives | 1270881 |

---

# Important Note

Large generated files are **not pushed to GitHub**.

Each team member should generate these files locally after cloning or pulling the repository.

```bash
python src/features/feature_engineering.py
python src/models/train_model.py
```

These commands will generate:

- Processed dataset
- Trained Random Forest model
- Model metadata
- Feature list

---

# Project Structure

```text
real_time_fraud_detection/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── docker/
│   └── docker-compose.yml
│
├── notebooks/
│   ├── 01_dataset_understanding.ipynb
│   ├── 02_eda_data_cleaning.ipynb
│   ├── 03_feature_engineering.ipynb
│   └── 04_model_training.ipynb
│
├── src/
│   ├── data_ingestion/
│   ├── preprocessing/
│   ├── features/
│   │   └── feature_engineering.py
│   ├── models/
│   │   └── train_model.py
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

# Folder Purpose

- `data/raw` → Original PaySim dataset
- `data/processed` → Feature-engineered dataset
- `notebooks` → Data analysis and machine learning notebooks
- `src/data_ingestion` → Data loading scripts
- `src/preprocessing` → Data preprocessing scripts
- `src/features` → Feature engineering pipeline
- `src/models` → Model training scripts
- `src/streaming` → Kafka producer and streaming logic
- `src/database` → MongoDB connection and storage logic
- `src/api` → Future REST API implementation
- `dashboard` → Monitoring dashboard resources
- `reports` → Final documentation and reports
- `models/saved_model` → Saved machine learning models and metadata

---

# Next Steps

The next phase of the project focuses on real-time fraud prediction.

Upcoming tasks include:

1. Build the Kafka Consumer
2. Load the trained Random Forest model
3. Predict fraud in real time
4. Store fraud alerts in MongoDB
5. Build REST APIs using FastAPI
6. Develop monitoring dashboards
7. Integrate Grafana and Prometheus
8. Perform end-to-end testing
9. Complete deployment and project documentation

---

# Team Workflow

Before starting new work:

```bash
git pull origin main
```

If required, regenerate the local datasets and trained model:

```bash
python src/features/feature_engineering.py
python src/models/train_model.py
```

Always commit and push your completed work:

```bash
git add .
git commit -m "Your commit message"
git push origin main
```