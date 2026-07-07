# Model Training Report

## Project Name

**Real-Time Financial Fraud Detection Pipeline**

---

## Purpose of Model Training

The purpose of model training was to build a machine learning model that can identify fraudulent financial transactions from the PaySim dataset.

The dataset is highly imbalanced because fraud cases are very rare compared to normal transactions. Because of this, the model should not only focus on accuracy. It should also focus on:

- Precision
- Recall
- F1-score
- False positives
- False negatives

In fraud detection, recall is very important because missing a fraud transaction can create financial loss.

---

## Dataset Used

The project uses the PaySim synthetic financial transaction dataset.

### Dataset path

```text
data/raw/paysim_transactions.csv
```

### Processed dataset path

```text
data/processed/processed_transactions.csv
```

### Total records

```text
6,362,620 transactions
```

### Target column

```text
isFraud
```

### Target meaning

- `0` → Non-fraud transaction
- `1` → Fraud transaction

---

## Raw Dataset Columns

The original dataset contains these main columns:

- step
- type
- amount
- nameOrig
- oldbalanceOrg
- newbalanceOrig
- nameDest
- oldbalanceDest
- newbalanceDest
- isFraud
- isFlaggedFraud

---

## Feature Engineering Before Training

Before training the models, feature engineering was performed to create useful fraud-related features.

### Important features created

- transaction_id
- is_origin_customer
- is_dest_customer
- is_dest_merchant
- origin_balance_diff
- destination_balance_diff
- origin_balance_error
- destination_balance_error
- abs_origin_balance_error
- abs_destination_balance_error
- amount_to_oldbalanceOrg_ratio
- amount_to_oldbalanceDest_ratio
- is_zero_oldbalanceOrg
- is_zero_oldbalanceDest

Transaction type was converted into one-hot encoded columns:

- type_CASH_IN
- type_CASH_OUT
- type_DEBIT
- type_PAYMENT
- type_TRANSFER

---

## Features Removed Before Training

Some columns were removed before training because they were not suitable as model input.

```python
drop_columns = [
    "transaction_id",
    "type",
    "isFlaggedFraud",
    "is_origin_customer",
    "isFraud"
]
```

### Reason

- `transaction_id` is only an identifier.
- `type` is already converted into one-hot encoded columns.
- `isFlaggedFraud` flags only a very small number of fraud cases.
- `is_origin_customer` adds little useful information.
- `isFraud` is the target column and must not be included in the input features.

---

## Train-Test Split

The processed dataset was split into training and testing datasets.

Training data was used for learning.

Testing data was used for evaluating model performance.

```text
X_train shape: (5090096, 23)
X_test shape : (1272524, 23)
y_train shape: (5090096,)
y_test shape : (1272524,)
```

A stratified split was used to preserve the fraud-to-non-fraud ratio.

---

## Models Trained

The following machine learning models were trained and compared:

1. Logistic Regression
2. Decision Tree
3. Random Forest
4. HistGradientBoostingClassifier

---

## Why These Models Were Used

### Logistic Regression

Used as the baseline model.

It is simple, fast, and suitable for binary classification.

However, due to the highly imbalanced dataset, it produced many false positives.

### Decision Tree

Decision Tree learns rule-based patterns and is easy to interpret.

However, it may overfit the training data if not controlled.

### Random Forest

Random Forest combines multiple decision trees.

It performs well on structured datasets and provided the best fraud detection performance.

### HistGradientBoostingClassifier

HistGradientBoostingClassifier is a boosting-based ensemble model designed for large datasets.

It achieved strong performance, but Random Forest performed slightly better overall.

---

## Model Performance Results

| Model | Accuracy | Precision | Recall | F1 Score | False Positives | False Negatives |
|-------|---------:|----------:|-------:|---------:|----------------:|----------------:|
| Random Forest | 0.999997 | 1.000000 | 0.997565 | 0.998781 | 0 | 4 |
| Decision Tree | 0.999962 | 0.973856 | 0.997565 | 0.985568 | 44 | 4 |
| HistGradientBoosting | 0.999947 | 0.962419 | 0.997565 | 0.979677 | 64 | 4 |
| Logistic Regression | 0.941934 | 0.021308 | 0.978698 | 0.041709 | 73855 | 35 |

---

## Final Selected Model

The final selected model is:

```text
Random Forest
```

### Reason

Random Forest provided:

- Highest precision
- Very high recall
- Best F1-score
- Zero false positives
- Only four false negatives

This makes it highly suitable for fraud detection because it minimizes missed fraud cases while avoiding unnecessary alerts.

---

## Best Model Confusion Matrix

```text
[[1270881,      0],
 [      4,   1639]]
```

### Meaning

- True Negatives: **1,270,881**
- False Positives: **0**
- False Negatives: **4**
- True Positives: **1,639**

The model correctly detected 1,639 fraudulent transactions while missing only 4.

---

## Saved Model Files

The trained model artifacts are stored in:

```text
models/saved_model/
```

Saved files:

```text
fraud_detection_model.pkl
model_features.pkl
model_metadata.json
```

### Purpose

- `fraud_detection_model.pkl` stores the trained Random Forest model.
- `model_features.pkl` stores the feature column order.
- `model_metadata.json` stores model performance and training information.

---

## Training Script

The training script is located at:

```text
src/models/train_model.py
```

Run the script using:

```powershell
python src/models/train_model.py
```

The script loads the processed dataset, prepares the features, trains and compares models, selects the best-performing model, and saves the trained model artifacts.

---

## Final Learning

The model training stage transformed the processed transaction data into a machine learning model capable of detecting fraudulent transactions.

The key learning from this stage is that **accuracy alone is not sufficient for fraud detection**. Since fraudulent transactions are rare, metrics such as **precision, recall, F1-score, false positives, and false negatives** are more important for evaluating model performance.

Random Forest was selected because it provided the best overall balance across these evaluation metrics.