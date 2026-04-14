<div align="center">

# 🪙 Real-Time Crypto Data Lakehouse

**An end-to-end streaming & batch data engineering platform built on the Medallion Architecture**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.8-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.x-003366?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Google Cloud Storage](https://img.shields.io/badge/Google_Cloud-GCS-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)](https://cloud.google.com/storage)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.8+-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Trino](https://img.shields.io/badge/Trino-432-DD00A1?style=for-the-badge&logo=trino&logoColor=white)](https://trino.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

> Ingests **Top-50 Binance USDT pairs** in real-time via WebSocket, stores historical OHLCV klines from the REST API,
> processes everything through a **Bronze → Silver → Gold** Delta Lake pipeline on **Google Cloud Storage (GCS)**,
> orchestrated heavily via **Apache Airflow (Data-Aware Scheduling)**, and serves analytics via **Trino** + **Power BI**.

**🔗 GitHub Repository:** [github.com/Quocanh1508/Crypto-DataLakehouse-Project](https://github.com/Quocanh1508/Crypto-DataLakehouse-Project)

</div>

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Flow](#-data-flow)
- [Infrastructure Services](#-infrastructure-services)
- [Quick Start](#-quick-start)
  - [Prerequisites](#prerequisites)
  - [Phase 1 – Start Infrastructure](#phase-1--start-infrastructure)
  - [Phase 2 – Run Ingestion](#phase-2--run-ingestion)
  - [Phase 3 – Spark Processing](#phase-3--spark-processing)
  - [Phase 4 – Query with Trino](#phase-4--query-with-trino)
- [Configuration](#-configuration)
- [Roadmap](#-roadmap)
- [Author](#-author)

---

## 🔍 Overview

This project implements a **production-grade Data Lakehouse** for real-time cryptocurrency market analysis. It combines two ingestion strategies:

| Mode | Source | Target | Frequency |
|---|---|---|---|
| **Stream** | Binance WebSocket (`@trade`) | Kafka → Bronze (Delta) | Tick-by-tick, real-time |
| **Batch** | Binance REST `/api/v3/klines` | GCS `raw-batch/` | On-demand / scheduled |

Data flows through a three-tier **Medallion Architecture**:

- 🥉 **Bronze** — Raw, immutable, partitioned Delta tables. No transformations.
- 🥈 **Silver** — Deduplicated, typed, validated records. Schema-enforced.
- 🥇 **Gold** — Business-ready OHLCV aggregates with moving averages (1m / 5m windows), ready for BI.

---

## 🏛 Architecture

```
=============================================================================================
                  REAL-TIME CRYPTO DATA LAKEHOUSE ARCHITECTURE (TOP 50)
=============================================================================================

[EXTERNAL SOURCES]                     [INGESTION LAYER - Phase 2]
       │
 ┌────────────┐                         ┌──────────────────────────────────────────────┐
 │ Binance WS ├─────(Top 10 Trades)────►│ Python Stream Producer (rel, auto-retry)   │
 └────────────┘                         └──────────────────────┬───────────────────────┘
       │                                                       │ (JSON, real-time)
 ┌────────────┐                         ┌──────────────────────▼───────────────────────┐
 │Binance REST├─────(Top 10 Klines)────►│ Apache Kafka (Topic: crypto_trades_raw)      │
 └────────────┘                         │ (Managed by Zookeeper)                       │
                                        └──────────────────────┬───────────────────────┘
                                                               │
═══════════════════════════════════════════════════════════════│═════════════════════════════
                                                               │
[PROCESSING & STORAGE LAYER (LAKEHOUSE) - Phase 3]             │
                                                               ▼
 ┌─────────────────────────────────────────────────────────────────────────────────────┐
 │ APACHE SPARK CLUSTER (Master & Workers with fine-tuned JVM limits)                  │
 │   │                                                                                 │
 │   ├─ 1. Core Streaming Job ─────(Kafka → GCS)──► [ BRONZE LAYER ] (Raw Append-Only) │
 │   │                                                                                 │
 │   ├─ 2. Micro-Batch Upsert ──(DQ Rules/Dedupe)─► [ QUARANTINE ] & [ SILVER LAYER ]  │
 │   │                                                                                 │
 │   └─ 3. Batch Aggregation ───────────(OHLCV)───► [ GOLD LAYER ]   (Business Ready)  │
 └─────────────────┬───────────────────────────────────────────────────┬───────────────┘
                   │ (Write: Delta Format, Upsert MERGE)               │
                   ▼                                                   │
 ┌──────────────────────────────────────────────────┐                  │ (Metadata Sync)
 │ GOOGLE CLOUD STORAGE (GCS)                       │                  ▼
 │ gs://crypto-lakehouse-group8/{bronze,silver,gold}│◄───────┐ ┌───────────────┐
 └──────────────────────────────────────────────────┘        │ │ HIVE METASTORE│
                    ▲                                         │ └───────┬───────┘
                    │ (Read: gs:// Protocol / ADC Auth)       │         │ (Store Schema)
═══════════════════│═════════════════════════════════════════│═════════▼═════════════════════
                   │                                         │  ┌────────────┐
[SERVING LAYER]    │                                         └──┤ POSTGRESQL │
                   │                                            └────────────┘
 ┌─────────────────┴────────────────────────────────┐                  ▲
 │ TRINO (Distributed SQL Query Engine)             ├──────────────────┘
 │ (Delta Lake Connector mapped to Hive Metastore)  │ (Fetch Schema/Partitions)
 └─────────────────┬────────────────────────────────┘
                   │
═══════════════════│═════════════════════════════════════════════════════════════════════════
                   │
[CONSUMPTION & MONITORING] (DirectQuery / ODBC)      [ORCHESTRATION - Phase 4]
                   ▼                                           
 ┌──────────────────────────────────┐                   ┌──────────────────────────────┐
 │ POWER BI                         │                   │ APACHE AIRFLOW               │
 │ (Live Ticker Dashboard, Top 50)  │                   │ - Schedule Silver/Gold Jobs  │
 ├──────────────────────────────────┤                   │ - Schedule Delta COMPACTION  │
 │ GRAFANA / PROMETHEUS / GCS LOGS  │                   └──────────────────────────────┘
 │ (Kafka Lag, Spark JVM, GCS I/O)  │
 └──────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| **Message Broker** | Apache Kafka (Confluent) | 7.5.0 | Real-time trade tick streaming |
| **Coordination** | Apache ZooKeeper | 7.5.0 | Kafka cluster management |
| **Object Storage** | Google Cloud Storage | Cloud | Fully managed cloud data lake (`ADC` Auth) |
| **Processing** | Apache Spark | 3.5.8 | Streaming & Incremental batch ETL engine |
| **Table Format** | Delta Lake | 3.x | ACID transactions on object storage |
| **Metastore** | Hive Metastore (Starburst) | 3.1.2-e.18 | Table schema & metadata catalog |
| **Metastore DB** | PostgreSQL | 15-alpine | HMS backend database |
| **Query Engine** | Trino | 432 | Federated SQL over Delta Lake |
| **Ingestion** | Python | 3.10+ | WebSocket & REST producers |
| **Orchestration** | Apache Airflow | 2.8+ | DAGs & Data-Aware Scheduling (Datasets) |
| **Visualization** | Power BI | — | Business intelligence dashboards |
| **Containerization** | Docker Compose | v3.8 | Local deployment with Resource Control |

---

## 📁 Project Structure

```
FinalProject/
│
├── 📄 docker-compose.yml           # Full stack: Kafka, Spark, Trino, HMS, PG
├── 📄 .env.example                 # Environment variable template → copy to .env
├── 📄 .gitignore                   # Excludes data dirs, venvs, secrets
│
├── 📂 ingestion/                   # Python data producers
│   ├── producer_stream.py          # WebSocket Top-50 USDT pairs → Kafka
│   │                               #   · Auto-reconnect via tenacity (exp. backoff)
│   │                               #   · Dead-letter queue for malformed ticks
│   │                               #   · rel dispatcher for WebSocket heartbeats
│   └── requirements.txt            # kafka-python, websocket-client, tenacity…
│
├── 📂 spark/                       # Custom Spark image
│   ├── Dockerfile                  # Spark 3.5.8 + Delta Lake 3.2.1 + GCS Connector
│   └── start-spark.sh              # Entrypoint for master / worker roles (LF-safe)
│
├── 📂 trino/
│   └── catalog/
│       └── delta.properties        # Trino → Delta Lake connector config (via HMS)
│
├── 📂 hive/
│   └── hive-site.xml               # HMS config: JDBC → PostgreSQL
│
├── 📂 processing/                  # Phase 3: Spark ETL jobs ✅ COMPLETE
│   ├── bronze_streaming.py         # Structured streaming: Kafka → Bronze (GCS Delta)
│   ├── bronze_to_silver.py         # Micro-batch: Incremental Upsert + Data Quality Rules
│   ├── silver_to_gold.py           # Batch: OHLCV Aggregator (1m/15m intervals)
│   └── requirements.txt            # pyspark, delta-spark, kafka-python
│
├── 📂 tests/                       # Pipeline validation suite ✅ COMPLETE
│   └── validate_pipeline.py        # Data integrity, latency, precision, dedup tests
│
├── 📂 cloud/                       # GCP provisioning scripts
│   └── gcs_setup.ps1               # Bucket + IAM setup for GCS
│
├── 📂 orchestration/               # Phase 5: Airflow Orchestration ✅ COMPLETE
│   └── dags/
│       ├── 02_bronze_streaming_dag.py  # Continuous Kafka->Bronze executor
│       ├── 03_silver_dag.py            # Event-triggered/scheduled micro-batch MERGE
│       └── 04_gold_dag.py              # Auto-triggered via Airflow Datasets (Silver completion)
│
└── 📂 appDataDir/                  # Local system logs & memory tuning configs
```

---

## 🔄 Data Flow

### Stream Path (Real-Time)

```
Binance WS (@trade tick)
  │
  ├─ Validate required fields: {e, E, s, t, p, q, T, m}
  ├─ Enrich with ingested_at timestamp
  │
  ├─ ✅ Valid   → Kafka topic: crypto_trades_raw  (keyed by symbol)
  └─ ❌ Invalid → Kafka topic: crypto_trades_dlq  (dead-letter queue)

Spark Structured Streaming & Airflow Batch Processing
  └─ Kafka source → Bronze (Delta, continuous append-only)
                 → Silver (Micro-batch with `availableNow=True`, DQ Quarantine splits, MERGE Upsert)
                 → Gold   (Scheduled via Airflow Datasets, OHLCV 1m/15m aggregations)
```

### Batch Path (Historical)

```
Binance REST /api/v3/klines
  │  Top-50 USDT pairs · 1000 candles each · 1-minute interval
  │  Rate-limit budget: 1100 weight / 1200 max
  │
  └─ GCS: gs://crypto-lakehouse-group8/raw-batch/<SYMBOL>/<DATE>/klines.csv

Spark Batch Job
  └─ raw-batch → Bronze → Silver → Gold (same medallion path)
```

---

## 🐳 Infrastructure Services

| Service | Container | Port(s) | Memory | Notes |
|---|---|---|---|---|
| **ZooKeeper** | `zookeeper` | 2181 | 512 MB | Kafka coordination |
| **Kafka** | `kafka` | 9092 (host), 29092 (internal) | 1 GB | Auto topic creation enabled |
| **Kafka Connect** | `kafka-connect` | 8083 | 1 GB | HTTP source connector |
| **PostgreSQL** | `postgres` | 5432 | 512 MB | Hive Metastore backend |
| **Hive Metastore** | `hive-metastore` | 9083 | 512 MB | Table catalog for Trino + Spark |
| **Trino** | `trino` | 8080 | 2 GB | Federated SQL query engine |
| **Spark Master** | `spark-master` | 7077, 8082 (UI) | 1 GB | Cluster manager |
| **Spark Worker** | `spark-worker` | — | 2 GB | 2 cores, 1.5 GB executor memory |

**Google Cloud Storage (GCS) Paths:**

| Path | Purpose |
|---|---|
| `gs://crypto-lakehouse-group8/bronze` | Raw Delta Lake tables (streaming) |
| `gs://crypto-lakehouse-group8/silver` | Cleaned & deduplicated Delta tables |
| `gs://crypto-lakehouse-group8/gold` | OHLCV aggregations & business metrics |
| `gs://crypto-lakehouse-group8/checkpoints` | Spark Structured Streaming checkpoints |

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** ≥ 24.0 (WSL2 backend on Windows)
- **Python** 3.10+
- **≥ 10 GB RAM** available for Docker
- **≥ 20 GB disk** (Spark image is large)

---

### Phase 1 – Start Infrastructure

```bash
# 1. Clone the repository
git clone https://github.com/Quocanh1508/Crypto-DataLakehouse-Project.git
cd Crypto-DataLakehouse-Project

# 2. Copy and configure secrets
cp .env.example .env
# Edit .env if needed (defaults work out-of-the-box for local dev)

# 3. Build custom Spark image + bring up all services
docker-compose up -d --build

# 4. Wait ~60-90 seconds, then verify all containers
docker ps
```

#### ✅ Phase 1 Verification Checklist

| Service | URL / Check | Expected Result |
|---|---|---|
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) (`admin` / `admin123`) | 5 buckets: bronze, silver, gold, checkpoints, raw-batch |
| **Trino UI** | [http://localhost:8080](http://localhost:8080) | Query editor accessible, 0 running queries |
| **Spark Master UI** | [http://localhost:8082](http://localhost:8082) | 1 worker registered, status ALIVE |
| **Kafka** | `docker logs kafka \| tail -5` | `started (kafka.server.KafkaServer)` |
| **Hive Metastore** | `docker logs hive-metastore \| tail -5` | `Starting Hive Metastore Server` |
| **PostgreSQL** | `docker exec postgres pg_isready -U hive` | `accepting connections` |

---

### Phase 2 – Run Ingestion

#### Setup Python environment

```bash
cd ingestion
python -m venv .venv

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
```

#### Set environment variables (Windows PowerShell)

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:MINIO_ENDPOINT          = "http://localhost:9000"
$env:MINIO_ACCESS_KEY        = "admin"
$env:MINIO_SECRET_KEY        = "admin123"
$env:BINANCE_REST_URL        = "https://api.binance.com"
$env:BINANCE_WS_URL          = "wss://stream.binance.com:9443/stream"
$env:TOP_N_COINS             = "50"
```

#### Run Batch Producer — Historical klines → MinIO

```bash
python ingestion/producer_batch.py
```

> ✅ **Expected**: CSVs appear in MinIO `raw-batch` bucket under `history/<SYMBOL>/<DATE>/klines.csv`
> Processes 50 symbols, ~2 weight each, with automatic rate-limit throttling at 1100/1200.

#### Run Stream Producer — Real-time WebSocket → Kafka

```bash
python ingestion/producer_stream.py
```

> ✅ **Expected**: Continuous trade ticks flow into `crypto_trades_raw` Kafka topic.
> Features exponential backoff retry (2s → 60s, up to 20 attempts) and heartbeat pings every 60s.

#### Verify Kafka messages

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto_trades_raw \
  --from-beginning \
  --max-messages 5
```

> ✅ **Expected**: JSON trade ticks with fields `e`, `s`, `p`, `q`, `T`, `ingested_at`, etc.

---

### Phase 3 – Spark Processing ✅ COMPLETE

> ✅ **Validated** — Bronze streaming and Silver upserts running on Spark 3.5.8 → GCS Delta Lake.

```bash
# Run Bronze streaming job (Kafka → GCS Delta)
docker run --rm --network finalproject_lakehouse-net \
  -v "${PWD}/processing:/processing" \
  -v "${env:APPDATA}\gcloud:/home/spark/.config/gcloud:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/home/spark/.config/gcloud/application_default_credentials.json \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  finalproject-spark-master:latest \
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /processing/bronze_streaming.py
```

---

### Phase 4 – Query with Trino

Navigate to [http://localhost:8080](http://localhost:8080) and run SQL:

```sql
-- List all Delta Lake tables
SHOW TABLES FROM delta.default;

-- Query Gold layer OHLCV aggregates
SELECT
    symbol,
    window_start,
    open, high, low, close,
    volume,
    ma_5m, ma_15m
FROM delta.default.gold_ohlcv
WHERE symbol = 'BTCUSDT'
ORDER BY window_start DESC
LIMIT 100;
```

---

## ⚙️ Configuration

All secrets and configuration are managed via `.env` (copy from `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC_RAW` | `crypto_trades_raw` | Main streaming topic |
| `KAFKA_TOPIC_DLQ` | `crypto_trades_dlq` | Dead-letter queue topic |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO S3-compatible endpoint |
| `MINIO_ACCESS_KEY` | `admin` | MinIO access key |
| `MINIO_SECRET_KEY` | `admin123` | MinIO secret key |
| `BINANCE_REST_URL` | `https://api.binance.com` | Binance REST base URL |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443/stream` | Binance combined stream URL |
| `TOP_N_COINS` | `50` | Number of top USDT pairs to track |

---

## 🗺 Roadmap

- [x] **Phase 1** — Dockerized infrastructure (Kafka, Spark, Trino, HMS, PostgreSQL)
- [x] **Phase 2** — Real-time WebSocket producer streaming to Kafka
- [x] **Phase 3** — Spark 3.5.8: Bronze continuous streaming + Silver upserts on GCS Delta Lake
- [x] **Phase 4** — Gold Layer OHLCV aggregation with Spark SQL (1m & 15m intervals)
- [x] **Phase 5** — Apache Airflow DAG orchestration (Dataset-driven DAGs & bash operators)
- [x] **Phase 6** — Infrastructure Limit Tuning (Resolved OOM 137, Memory footprint constraints)
- [ ] **Phase 7** — Power BI / Trino dashboards for analytics consumption
- [ ] **Phase 8** — dbt data quality models on Gold layer (Optional enhancements)

---

## 👤 Author

**Quoc Anh Nguyen**
- GitHub: [@Quocanh1508](https://github.com/Quocanh1508)
- Email: quocanh0815@gmail.com
- Project: [Crypto-DataLakehouse-Project](https://github.com/Quocanh1508/Crypto-DataLakehouse-Project)

---

<div align="center">

Made with ❤️ for the **Big Data & Analytics** Final Project

*Built on open-source: Apache Kafka · Apache Spark · Delta Lake · Trino · MinIO*

</div>
