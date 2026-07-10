# Spark Structured Streaming Consumer Report

## Overview

**File:** `src/streaming/spark_streaming_consumer.py`  
**Kafka Topic:** `transactions`  
**Test Limit:** 20 transactions  
**Status:** Successfully completed  

This component reads live transaction messages from Kafka using PySpark Structured Streaming, parses the incoming JSON data and displays selected transaction fields in Spark micro-batches.

---

## Objective

The purpose of this script is to:

- connect Spark Structured Streaming to Kafka;
- subscribe to the `transactions` topic;
- parse transaction JSON using a Spark schema;
- display selected transaction fields;
- use checkpointing for local streaming state.

---

## Streaming Flow

```text
Kafka Topic
    |
    v
Spark Structured Streaming
    |
    v
JSON Parsing
    |
    v
Selected Transaction Columns
    |
    v
Console Micro-Batches
```

---

## Main Configuration

| Setting | Value |
|---|---|
| Kafka server | `localhost:9092` |
| Kafka topic | `transactions` |
| Output mode | `append` |
| Output format | `console` |
| Maximum displayed rows | `20` |
| Checkpoint folder | `checkpoints/spark_streaming_consumer` |

The script uses the Spark Kafka connector:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
```

---

## Output Columns

| Column | Description |
|---|---|
| `transaction_id` | Sequential ID added by the producer |
| `step` | PaySim simulation step |
| `type` | Transaction type |
| `amount` | Transaction amount |
| `nameOrig` | Origin account |
| `nameDest` | Destination account |
| `isFraud` | Original fraud label |
| `stream_timestamp` | Producer streaming timestamp |

---

## Checkpoint Handling

The script uses:

```python
CHECKPOINT_DIR = ROOT_DIR / "checkpoints" / "spark_streaming_consumer"
```

The previous checkpoint is cleaned before a fresh local test. This prevents old Kafka offsets from affecting the new run.

The checkpoint folder should remain excluded from Git:

```gitignore
checkpoints/
```

---

## Windows Hadoop Fix

The first Spark run failed with:

```text
NativeIO$Windows.access0
```

The issue was resolved by configuring a local Hadoop folder containing:

```text
winutils.exe
hadoop.dll
```

After this setup, Spark successfully created checkpoints and processed Kafka messages.

---

## Test Commands

### Terminal 1 — Spark Consumer

```powershell
python .\src\streaming\spark_streaming_consumer.py
```

### Terminal 2 — Kafka Producer

```powershell
python .\src\streaming\producer.py --limit 20 --sleep 0.5
```

The producer confirmed:

```text
Streaming completed. Total transactions sent: 20
```

---

## Spark Batch Summary

| Batch | Records |
|---:|---:|
| Batch 0 | 0 |
| Batch 1 | 1 |
| Batch 2 | 10 |
| Batch 3 | 3 |
| Batch 4 | 3 |
| Batch 5 | 2 |
| Batch 6 | 1 |
| **Total** | **20** |

`Batch 0` was empty because the Spark consumer started before the producer sent any messages.

---

## Representative Output

| Transaction ID | Type | Amount | Fraud Label |
|---:|---|---:|---:|
| 1 | PAYMENT | 9839.64 | 0 |
| 2 | PAYMENT | 1864.28 | 0 |
| 3 | TRANSFER | 181.00 | 1 |
| 4 | CASH_OUT | 181.00 | 1 |
| 10 | DEBIT | 5337.77 | 0 |
| 16 | CASH_OUT | 229133.94 | 0 |
| 20 | TRANSFER | 215310.30 | 0 |

The test included PAYMENT, DEBIT, CASH_OUT and TRANSFER transactions.

Two fraud-labelled records were visible:

| Transaction ID | Type | Amount |
|---:|---|---:|
| 3 | TRANSFER | 181.00 |
| 4 | CASH_OUT | 181.00 |

---

## Result

The test confirmed that:

- Spark connected to Kafka successfully;
- the `transactions` topic was consumed;
- JSON messages were parsed correctly;
- 20 transactions were processed;
- records were displayed in Spark micro-batches;
- fraud labels and timestamps were preserved;
- checkpointing worked;
- the Windows Hadoop error was resolved.

---

## Conclusion

The `spark_streaming_consumer.py` component is complete.

It successfully performs:

```text
Kafka -> Spark Structured Streaming -> JSON Parsing -> Console Output
```

The next separate task is to apply the trained fraud model inside Spark and store prediction results in MongoDB.