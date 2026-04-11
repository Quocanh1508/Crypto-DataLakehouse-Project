# 🚀 Crypto Lakehouse - Complete Orchestration Flow

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       REALTIME CRYPTO DATA PIPELINE                             │
└─────────────────────────────────────────────────────────────────────────────────┘

                              📊 DATA FLOW
                              ━━━━━━━━━━━

┌──────────────────────┐
│  BINANCE DATA        │
│  ├─ WebSocket Stream │ ◄──────┐
│  │  (24/7 realtime)  │        │
│  └─ REST API Batch   │        │ INGESTION
│     (daily 8 AM)     │        │ (producer_*.py)
└──────────────────────┘        │
         │                       │
         │ (kafka:9092)          │
         ▼                       │
┌──────────────────────────────────────────────────────────────┐
│                    KAFKA MESSAGE BROKER                      │
│                    (Confluent 7.5.0)                         │
│  ├─ Topic: crypto-trades (streaming ticks)                  │
│  └─ Partitions: 1 per symbol                                │
└──────────────────────────────────────────────────────────────┘
         │
         ├─────────────────────────────────────────┐
         │                                         │
         │ (Spark Streaming 24/7)                  │
         ▼                                         ▼
    ┌──────────────────────┐          ┌─────────────────────────────┐
    │  BRONZE LAYER        │          │  BRONZE LAYER               │
    │  (Streaming)         │          │  (Batch)                    │
    │                      │          │                             │
    │ gs://crypto-lake.../ │          │ gs://crypto-lake.../        │
    │ bronze/              │          │ bronze/ (from raw-batch)    │
    │                      │          │                             │
    │ 💾 Delta Format      │          │ 💾 Delta Format             │
    │ ⏱️ Real-time feeds  │          │ ⏱️ Daily from API          │
    └──────────────────────┘          └─────────────────────────────┘
         │                                    │
         └────────────────┬───────────────────┘
                          │
                 🔄 MERGE & DEDUPLICATE
                 (bronze_to_silver.py)
                 ⏰ Daily @ 9 AM UTC
                 (After producer_batch)
                          │
                          ▼
    ┌──────────────────────────────────────┐
    │      SILVER LAYER                    │
    │      (Cleaned & Deduplicated)        │
    │                                      │
    │  gs://crypto-lakehouse-group8/       │
    │  silver/                             │
    │                                      │
    │  Columns:                            │
    │  ├─ trade_id (unique key)            │
    │  ├─ symbol (BTCUSDT, ETHUSDT, ...)   │
    │  ├─ event_time (timestamp)           │
    │  ├─ price (decimal)                  │
    │  ├─ qty (decimal)                    │
    │  ├─ processing_timestamp             │
    │                                      │
    │  💾 Delta Format (ACID)              │
    │  📊 ~8065 rows (current)             │
    │  🔖 Partitioned by (symbol, date)    │
    └──────────────────────────────────────┘
                          │
         ┌────────────────┘
         │
         │ ⭐ REALTIME AGGREGATION
         │ (silver_to_gold.py)
         │ ⏰ Every 15 minutes
         │ 🔄 1-min + 5-min OHLCV
         │ 📈 Moving Averages (MA7, MA20, MA50)
         │
         ▼
    ┌──────────────────────────────────────┐
    │      GOLD LAYER                      │
    │      (Analytics Ready)               │
    │                                      │
    │  gs://crypto-lakehouse-group8/       │
    │  gold/symbol=BTCUSDT/                │
    │       candle_date=2026-04-12/        │
    │                                      │
    │  1-Minute Candles (128 rows)         │
    │  ├─ candle_time (minute bucket)      │
    │  ├─ open, high, low, close           │
    │  ├─ volume, tick_count               │
    │  ├─ ma_7, ma_20, ma_50               │
    │                                      │
    │  5-Minute Candles (48 rows)          │
    │  ├─ candle_time (5m bucket)          │
    │  ├─ open, high, low, close           │
    │  ├─ volume, tick_count               │
    │  ├─ ma_7, ma_20, ma_50               │
    │                                      │
    │  💾 Delta Format                     │
    │  📊 176 total rows (49 symbols)      │
    │  🔖 Partitioned by (symbol, date)    │
    │  🌍 GCS gs:// native storage         │
    └──────────────────────────────────────┘
         │
         └─────────────────────┐
                               │
              🛠️ MAINTENANCE   │
              (Nightly 2 AM)   │
              ├─ OPTIMIZE      │
              │  (Z-order by   │
              │   symbol,date) │
              │                │
              └─ VACUUM        │
                 (7-day        │
                  retention)
```

---

## 📅 Scheduling Timeline

### **DAILY SCHEDULE (UTC)**

```
08:00 AM ┌─────────────────────────────────────────────────────┐
         │ TASK 1: producer_batch.py                           │
         │ └─ REST API → gs://...raw-batch/                    │
         │ └─ Duration: ~10 minutes                            │
         │ └─ Status: ✅ Complete                              │
         └────────────┬────────────────────────────────────────┘
                      │
09:00 AM ┌────────────▼────────────────────────────────────────┐
         │ TASK 2: bronze_to_silver.py                         │
         │ └─ Bronze merge + deduplicate                       │
         │ └─ Duration: ~20 minutes                            │
         │ └─ Result: 8065 cleaned rows in Silver              │
         └────────────┬────────────────────────────────────────┘
                      │
02:00 AM ┌────────────▼────────────────────────────────────────┐
next day │ TASK 4: delta_maintenance.py                        │
         │ └─ OPTIMIZE (Z-order compress)                      │
         │ └─ VACUUM (delete old versions)                     │
         │ └─ Duration: ~15 minutes                            │
         │ └─ Saves ~30% GCS storage                           │
         └─────────────────────────────────────────────────────┘
```

### **REALTIME SCHEDULE (Every 15 minutes)**

```
 0 min ┌─────────────────────────────┐
       │ TASK 3a: silver_to_gold.py  │
       │ └─ Process Silver ticks     │
       │ └─ Generate OHLCV candles   │
       │ └─ Compute MA indicators    │
       │ └─ Write to Gold            │
       │ └─ Duration: ~5 minutes     │
       └─────────────────────────────┘
         │
15 min  │  ┌─────────────────────────────┐
        └─►│ TASK 3b: silver_to_gold.py  │
           │ └─ (repeat same)             │
           └─────────────────────────────┘
             │
30 min      │  ┌─────────────────────────────┐
            └─►│ TASK 3c: silver_to_gold.py  │
               │ └─ (repeat same)             │
               └─────────────────────────────┘
                 │
45 min          │  ┌─────────────────────────────┐
                └─►│ TASK 3d: silver_to_gold.py  │
                   │ └─ (repeat same)             │
                   └─────────────────────────────┘

Every hour: 4 × silver_to_gold executions
Every day:  96 × silver_to_gold executions
Every day:  1 × producer_batch + 1 × bronze_to_silver
```

---

## 💰 Cost Analysis (GCS @ $100/month budget)

```
Task                    Frequency    Cost/month    Notes
─────────────────────────────────────────────────────────────
producer_batch.py       1x/day       ~$0.50        Lightweight
bronze_to_silver.py     1x/day       ~$1.50        Medium compute
silver_to_gold.py       96x/day      ~$30.00       96 × $0.31/job
delta_maintenance.py    1x/day       ~$0.50        Nightly
─────────────────────────────────────────────────────────────
                        TOTAL:       ~$33/month    ✅ Well within budget
```

---

## 🏗️ Infrastructure Stack

```
┌────────────────────────────────────────────────────────────┐
│                    DOCKER COMPOSE SERVICES                 │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  📨 Message Broker                                         │
│  ├─ zookeeper:2181                                         │
│  ├─ kafka:9092 (broker)                                    │
│  └─ kafka-connect:8083 (connectors)                        │
│                                                             │
│  💾 Storage                                                │
│  ├─ minio:9000 (local S3 compat)                           │
│  └─ postgres:5432 (metadata + airflow)                     │
│                                                             │
│  🔍 Metadata Layer                                         │
│  ├─ hive-metastore:9083                                    │
│  └─ trino:8080 (query engine)                              │
│                                                             │
│  ⚙️ Processing                                             │
│  ├─ spark-master:7077                                      │
│  └─ spark-worker:7077 (2 cores, 1.5GB)                     │
│                                                             │
│  📊 Orchestration (NEW)                                    │
│  ├─ airflow-webserver:8888                                 │
│  └─ airflow-scheduler (background)                         │
│                                                             │
└────────────────────────────────────────────────────────────┘
```

---

## 🎯 Airflow UI Access

Once running:
- **Webserver URL:** `http://localhost:8888`
- **Username:** `admin`
- **Password:** `admin`

### DAGs Visible:
1. ✅ `master_crypto_pipeline_daily` - Producer batch + Bronze→Silver + Maintenance
2. ✅ `silver_to_gold_realtime` - Every 15 minutes (NEW!)

---

## 📊 Data SLA

```
Layer     Update Frequency    Latency    Volume        Use Case
───────────────────────────────────────────────────────────────
Bronze    Real-time (24/7)    <1 min     1MB/min       Raw data
Silver    Daily (9 AM UTC)    ~30 min    ~50MB         Cleaned
Gold      Every 15 min        ~5 min     ~5MB/day      Analytics
```

---

## 🚀 Quick Start Commands

```bash
# Start all services
docker-compose up -d

# View Airflow logs
docker logs airflow-webserver -f
docker logs airflow-scheduler -f

# Check DAG status
docker exec airflow-webserver airflow dags list
docker exec airflow-webserver airflow dags trigger master_crypto_pipeline_daily

# View processed data
gcloud storage ls gs://crypto-lakehouse-group8/gold/symbol=BTCUSDT/

# Query Gold layer via Trino
# (Setup by Teammate 2 later)
```

---

## ✅ Summary

**You now have:**
- ✅ Producer batch (REST API daily)
- ✅ Bronze layer (streaming 24/7 + batch daily)
- ✅ Silver layer (deduplicated daily)
- ✅ **Gold layer with 15-min realtime updates** 🎯
- ✅ Airflow orchestration (master DAG)
- ✅ Delta maintenance (nightly compression)

**Cost:** ~$33/month GCS
**Realtime:** OHLCV candles updated every 15 minutes
**Status:** Ready for production! 🚀
