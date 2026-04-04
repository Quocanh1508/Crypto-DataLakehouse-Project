# 🚀 Crypto Lakehouse - Teammate Onboarding & Cloud Access Guide

Welcome! We've successfully fully migrated the local Kafka streaming architecture directly into the cloud. The Bronze and Silver processing layers now natively use Google Cloud Storage (GCS) and Delta Lake. 

Since all storage operations rely heavily on IAM access natively within GCP, you MUST properly authenticate your local environment before any Spark or Kafka Connect containers will work. 

## 1. Setting up Google Cloud Locally 

You need the `gcloud` CLI tool installed. Once installed, log in locally.

```bash
# 1. Login with your Google Account associated with the project
gcloud auth login

# 2. Set the default project
gcloud config set project crypto-lakehouse-group8

# 3. Generate the Application Default Credentials (ADC) Token
gcloud auth application-default login
```

> [!IMPORTANT]  
> After running the ADC login, Google will generate a secure OAuth JSON file.
> 
> **Windows path:** `%APPDATA%\gcloud\application_default_credentials.json`
> **Mac/Linux path:** `~/.config/gcloud/application_default_credentials.json`

Because we run the pipeline inside Docker out-of-the-box, the underlying Spark execution environment has ZERO access to your host machine's cloud credentials unless explicitly mapped into the container.

## 2. Docker Execution & Credentials Mount 🛡️

To route your cloud tokens securely into Spark (running natively via Linux internal UID `1000` / `spark`), apply the precise bind mount logic:

### Spark Submit Example
If you are initiating pipelines directly outside of `docker-compose`:

```bash
# Provide the host APPDATA path specifically mapping into /home/spark/.config
# Note: Spark runs safely as a non-root user. Do NOT map to /root/.config
docker run --rm \
  --network finalproject_lakehouse-net \
  -v "${PWD}/processing:/processing" \
  -v "${env:APPDATA}\gcloud:/home/spark/.config/gcloud:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/home/spark/.config/gcloud/application_default_credentials.json \
  ...
```

### Docker Compose
In production/local multi-service architecture, your services are correctly pre-configured inside `docker-compose.yml`. Keep your JSON file intact so the environment bindings can successfully authenticate.

## 3. Spark & GCS "Nuclear" Compatibility ☢️

Due to legacy classpath collisions and internal limitations within Hadoop 3.4.x executing inside Delta Lake frameworks, we forcibly hijack native Google service account detection routines safely utilizing your localized OAuth user credentials!

If you build new pipelines or write arbitrary queries accessing Spark directly, ALWAYS copy the exact Spark Builder logic verified in `validate_pipeline.py`.

```python
# The validated Spark Delta GCP Setup (Do NOT alter these specific string injections):
.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
.config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
.config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/home/spark/.config/gcloud/application_default_credentials.json")
.config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
```

## 4. Understanding Data Quality Upgrades 🔨

We identified that raw payload representations streaming directly from Binance generated duplicated case-sensitive schemas natively dropping Spark queries (`t` Trade ID vs `T` Trade Time).

* Our ingestion architecture intentionally bypasses these Spark limitations by executing localized `string replacements` directly against raw byte representations resolving all collisions **before** parsing.
* You do not need to drop columns before `split_valid_quarantine()`. Any broken streams bypassing structural schemas gracefully isolate inside the quarantine mechanism instead of dropping pipelines.

## 5. Monitoring Handoff (Grafana / Prometheus / UI) 📊

If you are setting up the monitoring stack, here are the critical integration points you need:

### Kafka Tracking
* **Broker Host**: `kafka:29092` (Internal Docker network), `localhost:9092` (Host machine).
* **Topic to monitor**: `crypto_trades_raw`
* **Metrics Goal**: Keep an eye on **Consumer Lag**. If the Spark polling falls behind the Binance WebSocket insertion rate, we risk data staleness. 

### Spark Execution Metrics
* **Spark Master UI**: `http://localhost:8080` (Monitors worker availability and memory/CPU allocation).
* **Spark Worker UI**: `http://localhost:8081`
* **Spark Application UI (Streaming)**: `http://localhost:4040` (Only active while `bronze_streaming.py` or `bronze_to_silver.py` is currently running).
* **Metrics Goal**: Monitor Executor memory (currently set to 1GB/2GB locally). Watch for `OutOfMemoryError` or failing micro-batches.

### GCP Storage (GCS)
* **Bronze Path**: `gs://crypto-lakehouse-group8/bronze`
* **Silver Path**: `gs://crypto-lakehouse-group8/silver`
* **Metrics Goal**: You can track storage size directly inside Google Cloud Metrics. Also, our `gcs_setup.ps1` granted `logging.logWriter` to the Service Account, so you can scrape Cloud Logging directly for any GCS API errors or Delta Lake lock timeouts.
