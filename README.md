# Real-Time Crypto Data Lakehouse

A **Medallion Architecture** Lakehouse streaming the **Top 50 Binance pairs** into Delta Lake on MinIO, queried by Trino, and visualized in Power BI.

## Quick Start

### Prerequisites
- Docker Desktop ≥ 24 (with **WSL2** backend on Windows)  
- Python 3.10+  
- 10 GB RAM available for Docker

---

## Phase 1: Start Infrastructure

```bash
# 1. Copy secrets file
cp .env.example .env

# 2. Bring up all services
docker-compose up -d

# 3. Wait ~60 seconds, then check all containers are healthy
docker ps
```

### ✅ Phase 1 Verification Checklist

| Service | URL / Check | Expected |
|---|---|---|
| **MinIO Console** | http://localhost:9001 (admin / admin123) | 5 buckets visible: bronze, silver, gold, checkpoints, raw-batch |
| **Kafka** | `docker logs kafka` | `started (kafka.server.KafkaServer)` |
| **Hive Metastore** | `docker logs hive-metastore` | `Starting Hive Metastore Server` |
| **Trino UI** | http://localhost:8080 | Query editor accessible |
| **Spark Master UI** | http://localhost:8082 | 1 worker registered |
| **PostgreSQL** | `docker exec postgres pg_isready -U hive` | `accepting connections` |

---

## Phase 2: Run Ingestion Scripts

### Setup Python environment

```bash
cd ingestion
pip install -r requirements.txt
```

### Copy .env values to shell (Windows PowerShell)

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "admin"
$env:MINIO_SECRET_KEY = "admin123"
$env:BINANCE_REST_URL = "https://api.binance.com"
$env:BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
```

### Run Batch Producer (historical klines → MinIO)

```bash
python ingestion/producer_batch.py
```
✅ **Expected**: CSVs appear in MinIO under `raw-batch/history/<SYMBOL>/`

### Run Stream Producer (real-time WebSocket → Kafka)

```bash
python ingestion/producer_stream.py
```
✅ **Expected**: Messages stream into Kafka topic `crypto_trades_raw`

### Verify Kafka messages

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto_trades_raw \
  --from-beginning \
  --max-messages 5
```

---

## Architecture

```
Binance WebSocket ──────────────────→ Kafka (crypto_trades_raw)
Binance REST API ────────────────────→ MinIO (raw-batch/history/)
                                          │
                                    Spark Streaming
                                          │
                              ┌───────────┴───────────┐
                           Bronze                    Silver
                     (Delta, partitioned)        (Deduped, typed)
                              │                        │
                           Gold (OHLCV + MA aggregates, 1m / 5m windows)
                              │
                           Trino ──────────────────→ Power BI
```

## Project Structure

```
FinalProject/
├── docker-compose.yml          # All services + mem_limits
├── .env.example                # Template — copy to .env
├── .gitignore
├── ingestion/
│   ├── producer_stream.py      # WebSocket Top-50 → Kafka
│   ├── producer_batch.py       # Historical klines → MinIO
│   └── requirements.txt
├── processing/                 # Phase 3 (Spark jobs)
├── orchestration/dags/         # Phase 4 (Airflow)
├── trino/catalog/
│   └── delta.properties        # Delta Lake connector config
└── hive/
    └── hive-site.xml           # HMS metadata config
```
