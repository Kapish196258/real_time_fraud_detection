# Real-Time Financial Fraud Detection Pipeline

The goal of this project is to build a real-time financial fraud detection pipeline that processes financial transactions, detects fraudulent activity using machine learning, and supports real-time fraud monitoring through streaming technologies.

The project combines **Data Engineering**, **Machine Learning**, **Apache Kafka**, **MongoDB**, and **Docker** to simulate an end-to-end fraud detection system using the PaySim dataset.

---

# Project Goal

The primary objective of this project is to detect fraudulent financial transactions using a scalable real-time data pipeline.

The project includes:

- Data Ingestion
- Data Cleaning and Preprocessing
- Exploratory Data Analysis (EDA)
- Feature Engineering
- Machine Learning Model Training
- Real-Time Transaction Streaming
- Kafka-Based Processing
- Fraud Prediction
- MongoDB Alert Storage
- Monitoring & Dashboarding
- Final Documentation

---

# Dataset

The project uses the **PaySim Synthetic Financial Dataset** for Mobile Money Transactions.

The dataset contains:

- Transaction Type
- Transaction Amount
- Sender & Receiver Account Balances
- Fraud Labels (`isFraud`)
- System Fraud Flags (`isFlaggedFraud`)

> **Note:** The dataset is not stored in this repository because of its large size. Each contributor should download it locally and place it inside:

```text
data/raw/paysim_transactions.csv
```

---

# Tech Stack

- Python
- Pandas
- NumPy
- Scikit-learn
- Apache Kafka
- Kafka UI
- MongoDB
- Mongo Express
- Docker & Docker Compose
- Git & GitHub

---

# Current Project Status

The project has successfully completed the offline data preparation and machine learning stages.

## Completed Work

- Project repository initialized
- Folder structure created
- Dataset understanding completed
- Exploratory Data Analysis (EDA) completed
- Data cleaning and validation completed
- Feature engineering notebook completed
- Feature engineering pipeline created
- Processed dataset generated
- Docker Compose infrastructure configured
- Apache Kafka configured
- Kafka UI configured
- MongoDB configured
- Mongo Express configured
- Kafka topic `transactions` created
- Python Kafka Producer implemented
- Transaction streaming tested successfully
- Model Training Notebook completed
- Model Training Pipeline Script completed
- Four machine learning models trained and compared
- Final Random Forest model selected
- Model Training Report added

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

Make sure **Docker Desktop** is running before executing the command.

---

# Current Data Pipeline

```text
PaySim Dataset
       │
       ▼
Feature Engineering
       │
       ▼
Processed Dataset
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
Fraud Prediction Model
       │
       ▼
MongoDB
       │
       ▼
Dashboard / Monitoring
```

---

# Machine Learning Models

The following models were trained and evaluated:

- Logistic Regression
- Decision Tree
- Random Forest
- HistGradientBoostingClassifier

## Final Selected Model

**Random Forest**

### Performance

| Metric | Value |
|---------|--------:|
| Accuracy | 0.999997 |
| Precision | 1.000000 |
| Recall | 0.997565 |
| F1-Score | 0.998781 |
| False Positives | 0 |
| False Negatives | 4 |
| True Positives | 1639 |
| True Negatives | 1270881 |

Random Forest achieved the best overall balance between precision, recall, F1-score, and false negatives, making it the most suitable model for fraud detection.

---

# Important Note

Large generated files are **not pushed to GitHub**.

Each contributor should generate them locally by running:

```bash
python src/features/feature_engineering.py
python src/models/train_model.py
```

These scripts generate:

- Processed Dataset
- Trained Random Forest Model
- Model Feature List
- Model Metadata

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
│   └── model_training.md
│
├── models/
│   └── saved_model/
│       ├── fraud_detection_model.pkl
│       ├── model_features.pkl
│       └── model_metadata.json
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

# Folder Purpose

| Folder | Purpose |
|---------|----------|
| `data/raw` | Stores the original PaySim dataset |
| `data/processed` | Stores the processed dataset |
| `notebooks` | Jupyter notebooks for project development |
| `src/data_ingestion` | Data loading scripts |
| `src/preprocessing` | Data preprocessing scripts |
| `src/features` | Feature engineering pipeline |
| `src/models` | Model training scripts |
| `src/streaming` | Kafka producer and streaming logic |
| `src/database` | MongoDB integration |
| `src/api` | Future REST API implementation |
| `dashboard` | Dashboard resources |
| `reports` | Project reports and documentation |
| `models/saved_model` | Trained model and metadata |

---

# Project Reports

The project documentation is available inside the **reports** folder.

Current report:

- `reports/model_training.md` — Complete documentation of the model training process, feature engineering summary, model comparison, evaluation metrics, confusion matrix, final Random Forest selection, and saved model artifacts.

---

# Next Steps

The next phase focuses on real-time fraud prediction.

Upcoming tasks:

1. Build the Kafka Consumer
2. Load the trained Random Forest model
3. Perform real-time fraud prediction
4. Store fraud alerts in MongoDB
5. Develop REST APIs using FastAPI
6. Build monitoring dashboards
7. Integrate Grafana and Prometheus
8. Perform end-to-end testing
9. Complete deployment and documentation

---

# Team Workflow

Before starting any work:

```bash
git pull origin main
```

Generate local files if required:

```bash
python src/features/feature_engineering.py
python src/models/train_model.py
```

Commit and push changes:

```bash
git add .
git commit -m "Your commit message"
git push origin main
```