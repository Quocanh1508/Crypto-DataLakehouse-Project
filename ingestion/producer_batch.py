"""
producer_batch.py
=================
Historical 1-minute Klines (OHLCV) REST Pull → Kafka

Steps:
  1. Re-use the same Top-10 symbol list as the streaming producer
  2. For each symbol, fetch klines from Binance /api/v3/klines
  3. Publish each kline row as a trade-compatible tick to Kafka topic: crypto_trades_raw
  4. Respects Binance rate-limit budget (1200 weight/min)

NOTE: This replaces the old MinIO/boto3 approach. By publishing directly to Kafka,
the batch data flows through the same Bronze streaming pipeline as realtime data,
ensuring consistent schema, deduplication, and partitioning handled by bronze_to_silver.py.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("batch_producer")

# ── Config ────────────────────────────────────────────────────────────────────
BINANCE_REST_URL  = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "crypto_trades_raw")
TOPIC_DLQ         = os.getenv("KAFKA_TOPIC_DLQ", "crypto_trades_dlq")
TOP_N             = int(os.getenv("TOP_N_COINS", "10"))

# Binance klines params
KLINE_INTERVAL    = "1m"    # 1-minute candles
KLINE_LIMIT       = 1000    # max per request (weight = 2)
WEIGHT_PER_REQ    = 2
WEIGHT_LIMIT      = 1100    # stay safely below 1200 limit
SLEEP_PER_SYMBOL  = 0.5     # seconds between symbols to avoid bursts


# ── Kafka Producer ─────────────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )


# ── Fetch Top-N symbols (same logic as streaming producer) ──────────────────
def fetch_top_symbols(n: int = TOP_N) -> list[str]:
    log.info("Fetching Top-%d symbols from Binance 24hr ticker...", n)
    resp = requests.get(f"{BINANCE_REST_URL}/api/v3/ticker/24hr", timeout=15)
    resp.raise_for_status()
    tickers = resp.json()
    usdt_pairs = [t for t in tickers if t["symbol"].endswith("USDT")]
    usdt_pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    symbols = [t["symbol"] for t in usdt_pairs[:n]]
    log.info("Top %d symbols: %s", n, symbols)
    return symbols


# ── Fetch klines for a single symbol ─────────────────────────────────────────
def fetch_klines(symbol: str) -> list[list]:
    url = f"{BINANCE_REST_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": KLINE_INTERVAL, "limit": KLINE_LIMIT}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ── Convert a kline row to a trade-compatible Kafka tick ─────────────────────
# We map OHLCV fields into the same schema as WebSocket trade ticks so that
# bronze_streaming.py can ingest them without schema changes.
def kline_to_tick(symbol: str, kline: list) -> dict:
    open_time_ms = int(kline[0])
    close_price  = str(kline[4])  # use close as representative price
    volume       = str(kline[5])
    num_trades   = int(kline[8])

    return {
        "e": "kline_batch",          # event_type: distinguishes batch from live ticks
        "E": open_time_ms,           # event_time_ms
        "s": symbol,                 # symbol
        "t": open_time_ms,           # trade_id (use open_time as synthetic id)
        "p": close_price,            # price
        "q": volume,                 # quantity
        "T": int(kline[6]),          # trade_time (close_time_ms)
        "m": False,                  # buyer_is_maker (not applicable for klines)
        "M": False,                  # ignore
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "batch_trades_count": num_trades,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    log.info("=== Batch Klines Producer (REST → Kafka) starting ===")
    producer = create_producer()
    symbols = fetch_top_symbols()

    total_weight = 0
    total_ticks  = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            log.info("[%d/%d] Fetching klines for %s...", i, len(symbols), symbol)
            klines = fetch_klines(symbol)

            for kline in klines:
                tick = kline_to_tick(symbol, kline)
                producer.send(TOPIC_RAW, key=symbol, value=tick)
                total_ticks += 1

            log.info("  → Published %d ticks for %s", len(klines), symbol)

            total_weight += WEIGHT_PER_REQ
            if total_weight >= WEIGHT_LIMIT:
                log.warning("Approaching rate limit (%d weight). Sleeping 60s...", total_weight)
                time.sleep(60)
                total_weight = 0
            else:
                time.sleep(SLEEP_PER_SYMBOL)

        except requests.HTTPError as exc:
            log.error("HTTP error for %s: %s — skipping", symbol, exc)
            producer.send(TOPIC_DLQ, value={"symbol": symbol, "error": str(exc)})
        except Exception as exc:
            log.error("Unexpected error for %s: %s — skipping", symbol, exc)

    producer.flush()
    producer.close()
    log.info("=== Batch ingestion complete. %d symbols, %d ticks published. ===",
             len(symbols), total_ticks)


if __name__ == "__main__":
    run()
