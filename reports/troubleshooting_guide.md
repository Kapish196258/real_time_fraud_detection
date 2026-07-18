## Troubleshooting

### Docker services are not starting

Make sure Docker Desktop is open and wait until the Docker Engine is running. Then restart the project services:

```powershell
docker compose -f .\docker\docker-compose.yml down
docker compose -f .\docker\docker-compose.yml up -d
docker compose -f .\docker\docker-compose.yml ps
```

Confirm that Kafka, Kafka UI, MongoDB, Mongo Express, Prometheus and Grafana are running.

### Python virtual environment is not activated

Activate the project virtual environment before running the producer, Spark pipeline or tests:

```powershell
.\venv\Scripts\Activate.ps1
```

If the required packages are missing, install them using:

```powershell
pip install -r .\requirements.txt
```

### PaySim dataset is not found

Confirm that the dataset is named:

```text
paysim_transactions.csv
```

and is stored at:

```text
data/raw/paysim_transactions.csv
```

The dataset should not be committed to GitHub because of its large size.

### Spark fails on Windows

If Spark shows an error related to `NativeIO$Windows.access0`, verify that the local Hadoop configuration is correct. The Hadoop `bin` folder should contain:

```text
winutils.exe
hadoop.dll
```

Confirm that `HADOOP_HOME` points to the correct local Hadoop folder and restart the terminal after changing environment variables.

### Spark does not process new Kafka messages

Check that Docker services are running, the Kafka `transactions` topic is available and the Spark pipeline is connected to the correct Kafka server.

For a fresh local demonstration, start Spark using:

```powershell
python -m src.streaming.spark_fraud_pipeline --reset-checkpoint
```

The checkpoint should only be reset when intentionally starting a fresh test.

### Producer sends an old transaction range

Use a new `--start-row` value for each validation run so that transaction ranges do not overlap.

Example:

```powershell
python .\src\streaming\producer.py --start-row 10700 --limit 200 --sleep 0.02
```

This sends 200 transactions starting after dataset row 10700.

### MongoDB contains existing transaction records

The project uses deterministic transaction IDs and MongoDB upserts. If the same transaction range is streamed again, the existing records are updated instead of creating duplicate records.

Use a different `--start-row` value when a completely new validation range is required.

### Grafana dashboards show no data

Confirm that:

- The Spark fraud pipeline is currently running.
- The metrics endpoint is available at `http://localhost:8000/metrics`.
- Prometheus is running at `http://localhost:9090`.
- Grafana is running at `http://localhost:3000`.
- The Grafana dashboard time range includes the latest streaming test.
- The dashboard has been refreshed after Spark completes the batch.

### Mongo Express does not open

Check the Docker service status:

```powershell
docker compose -f .\docker\docker-compose.yml ps
```

If MongoDB or Mongo Express is not running, restart the Docker services:

```powershell
docker compose -f .\docker\docker-compose.yml restart
```

### Safe demonstration order

For the final project demonstration, use this order:

1. Open Docker Desktop.
2. Start the Docker Compose services.
3. Open Kafka UI, Mongo Express, Prometheus and Grafana.
4. Start the Spark fraud pipeline.
5. Run the Python producer with a new transaction range.
6. Wait for Spark to complete the micro-batches.
7. Refresh MongoDB, Prometheus and Grafana.