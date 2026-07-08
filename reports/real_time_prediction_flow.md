# Real-Time Prediction Flow Report

## Purpose

This report explains how the saved fraud detection model is used for real-time prediction.

After model training, the project needed a reusable prediction flow so that new incoming transactions could be checked without opening the notebook again.

This is why `predict_fraud.py` was created.

---

## Main File

```text
src/models/predict_fraud.py
```

This file loads the saved model, prepares incoming transaction data, and returns fraud prediction output.

---

## Why This File Was Needed

During model training, the model learned from engineered and processed features.

But real-time transactions arrive in raw form with columns like:

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
```

The trained model cannot directly use raw transaction data.

So each new transaction must be converted into the same model-ready format used during training.

---

## Files Connected With This Flow

```text
models/saved_model/fraud_detection_model.pkl
models/saved_model/model_features.pkl
src/models/predict_fraud.py
src/streaming/consumer.py
```

Flow:

```text
Incoming transaction
    ↓
predict_fraud.py
    ↓
Load saved Random Forest model
    ↓
Create required engineered features
    ↓
Arrange columns in training order
    ↓
Predict fraud or non-fraud
    ↓
Return prediction result
```

---

## Saved Model Files Used

### `fraud_detection_model.pkl`

This file stores the final trained Random Forest model.

### `model_features.pkl`

This file stores the exact feature columns used during model training.

This is important because the model expects the same columns in the same order during prediction.

---

## Important Functions

### `load_model_artifacts()`

Loads the saved model and feature list.

If the files are missing, it shows a clear error asking the user to run model training first.

---

### `prepare_transaction_features()`

Converts one raw transaction into model-ready format.

It creates features such as:

```text
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
```

It also creates one-hot encoded transaction type columns:

```text
type_CASH_IN
type_CASH_OUT
type_DEBIT
type_PAYMENT
type_TRANSFER
```

---

### `predict_transaction()`

Takes one transaction dictionary and returns prediction output.

Example:

```python
{
    "transaction_id": 1,
    "prediction": 1,
    "fraud_probability": 0.98,
    "threshold": 0.5,
    "model_used": "RandomForestClassifier"
}
```

Meaning:

```text
prediction = 1 means fraud
prediction = 0 means non-fraud
fraud_probability means model risk score
threshold means cutoff used for fraud decision
model_used shows which model made the prediction
```

---

## Why Threshold Is Used

The model returns fraud probability.

If the threshold is `0.5`, then any transaction with fraud probability greater than or equal to `0.5` is predicted as fraud.

Example:

```text
fraud_probability = 0.82
0.82 >= 0.5 → fraud
```

The threshold can be adjusted later depending on business needs.

---

## Test Command

Run from project root:

```powershell
python src\models\predict_fraud.py
```

Expected output:

```text
A prediction dictionary should be printed in the terminal.
```

Example:

```text
{'transaction_id': 1, 'prediction': 1, 'fraud_probability': 1.0, 'threshold': 0.5, 'model_used': 'RandomForestClassifier'}
```

---

## Why This Step Is Important

This file connects the offline trained model with the real-time fraud detection pipeline.

Without this file, Kafka can stream transactions, but the system cannot prepare them properly for model prediction.

With this file, the Kafka consumer can send each transaction to the saved model and receive a fraud prediction.

---

## Final Understanding

The real-time prediction flow makes the trained Random Forest model reusable outside the notebook.

It ensures every incoming transaction goes through the same feature preparation process before prediction.