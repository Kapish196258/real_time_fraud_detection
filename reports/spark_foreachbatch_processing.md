# Spark `foreachBatch` Processing Report

## Overview

**Source file:** `src/streaming/spark_fraud_pipeline.py`  
**Kafka topic:** `transactions`  
**Processing method:** Spark Structured Streaming `foreachBatch()`  
**Test size:** 20 transactions  
**Status:** Successfully completed  

This stage validates that Kafka transactions can be received and processed inside a custom Spark micro-batch function.

---

## Objective

The purpose of this test was to verify that the new Spark fraud pipeline can:

- connect to Kafka;
- consume transaction messages;
- parse the incoming JSON records;
- process records through `foreachBatch()`;
- count records in each micro-batch;
- detect null transaction IDs;
- count fraud-labelled transactions;
- display selected transaction fields;
- calculate processing time and throughput.

Model prediction and MongoDB storage were intentionally excluded from this stage.

---

## Processing Flow

```text
Kafka Topic: transactions
        |
        v
Spark Structured Streaming
        |
        v
JSON Schema Parsing
        |
        v
foreachBatch()
        |
        +--> Record count
        +--> Null transaction-ID validation
        +--> Fraud-label count
        +--> Console table
        +--> Processing time
        +--> Throughput
```

---

## Test Commands

### Terminal 1 — Spark fraud pipeline

```powershell
python .\src\streaming\spark_fraud_pipeline.py --reset-checkpoint
```

### Terminal 2 — Kafka producer

```powershell
python .\src\streaming\producer.py --limit 20 --sleep 0.5
```

---

## Batch Validation Result

The Spark micro-batch completed successfully.

| Metric | Result |
|---|---:|
| Records processed in the displayed batch | 19 |
| Earlier transaction already processed | 1 |
| Total streamed transactions | 20 |
| Null transaction IDs | 0 |
| Fraud-labelled transactions | 2 |
| Processing time | 1.2746 seconds |
| Batch throughput | 14.91 records/second |
| Batch status | Completed |

The table shown in the terminal contained transaction IDs `2` through `20`. Transaction ID `1` had already been processed in an earlier micro-batch, giving a total of 20 transactions across the complete streaming run.

---

## Output Table

| Transaction ID | Step | Type | Amount | Origin Account | Destination Account | Fraud Label |
|---:|---:|---|---:|---|---|---:|
| 2 | 1 | PAYMENT | 1864.28 | `C1666544295` | `M2044282225` | 0 |
| 3 | 1 | TRANSFER | 181.00 | `C1305486145` | `C553264065` | 1 |
| 4 | 1 | CASH_OUT | 181.00 | `C840083671` | `C38997010` | 1 |
| 5 | 1 | PAYMENT | 11668.14 | `C2048537720` | `M1230701703` | 0 |
| 6 | 1 | PAYMENT | 7817.71 | `C90045638` | `M573487274` | 0 |
| 7 | 1 | PAYMENT | 7107.77 | `C154988899` | `M408069119` | 0 |
| 8 | 1 | PAYMENT | 7861.64 | `C1912850431` | `M633326333` | 0 |
| 9 | 1 | PAYMENT | 4824.36 | `C1265012928` | `M1176932104` | 0 |
| 10 | 1 | DEBIT | 5337.77 | `C712410124` | `C195600860` | 0 |
| 11 | 1 | DEBIT | 9644.94 | `C1900366749` | `C997608398` | 0 |
| 12 | 1 | PAYMENT | 3099.97 | `C249177573` | `M2096539129` | 0 |
| 13 | 1 | PAYMENT | 2560.74 | `C1648232591` | `M972865270` | 0 |
| 14 | 1 | PAYMENT | 11633.76 | `C1716932897` | `M801569151` | 0 |
| 15 | 1 | PAYMENT | 4098.78 | `C1026483832` | `M1635378213` | 0 |
| 16 | 1 | CASH_OUT | 229133.94 | `C905080434` | `C476402209` | 0 |
| 17 | 1 | PAYMENT | 1563.82 | `C761750706` | `M1731217984` | 0 |
| 18 | 1 | PAYMENT | 1157.86 | `C1237762639` | `M1877062907` | 0 |
| 19 | 1 | PAYMENT | 671.64 | `C2033524545` | `M473053293` | 0 |
| 20 | 1 | TRANSFER | 215310.30 | `C1670993182` | `C1100439041` | 0 |

> The output table above is reconstructed from the successful Spark terminal output.

---

## Fraud-Label Analysis

Two transactions contained `isFraud = 1`.

| Transaction ID | Type | Amount | Fraud Label |
|---:|---|---:|---:|
| 3 | TRANSFER | 181.00 | 1 |
| 4 | CASH_OUT | 181.00 | 1 |

This confirms that the Spark JSON parser preserved the original PaySim fraud label correctly.

The fraud-labelled records belong to `TRANSFER` and `CASH_OUT`, which is consistent with the PaySim dataset.

---

## Data Quality Validation

The pipeline reported:

```text
NULL TRANSACTION IDs : 0
```

This confirms that all parsed transactions in the batch had valid transaction IDs.

No malformed or unparsed transaction ID was detected in the successful run.

---

## Performance Analysis

The displayed batch processed 19 records in:

```text
1.2746 seconds
```

Calculated throughput:

```text
14.91 records/second
```

This throughput represents the processing speed of the current local test, including:

- Spark batch counting;
- null-ID validation;
- fraud-label counting;
- DataFrame sorting;
- console table rendering.

Console output adds noticeable overhead, so later performance tests without full table printing are expected to achieve higher throughput.

---

## Result Analysis

The test confirms that:

1. Kafka messages reached Spark successfully.
2. Spark parsed the JSON structure correctly.
3. `foreachBatch()` executed successfully.
4. Transaction IDs were preserved.
5. No null transaction IDs were found.
6. Fraud labels were preserved.
7. Multiple transaction types were processed.
8. Batch processing time was measured.
9. Throughput was calculated.
10. The batch completed without an exception.

This stage provides a stable foundation for adding model prediction and MongoDB writes.

---

## Conclusion

The Spark `foreachBatch()` processing stage was completed successfully.

The pipeline processed all 20 streamed transactions across the complete run, validated transaction IDs, identified two fraud-labelled records and measured local batch performance.

The next development stage is to load the saved Random Forest model, prepare the trained feature set and generate fraud predictions for each Spark micro-batch.