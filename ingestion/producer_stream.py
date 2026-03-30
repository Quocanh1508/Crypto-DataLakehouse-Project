"""
producer_stream.py
==================
Dynamic Top-50 Binance WebSocket → Kafka Producer

Steps:
  1. Fetch /api/v3/ticker/24hr → sort by quoteVolume desc → take top 50
  2. Build combined WebSocket URL
  3. Publish each trade tick to Kafka topic: crypto_trades_raw
  4. Route malformed messages to: crypto_trades_dlq (dead-letter queue)
  5. Auto-reconnect with exponential backoff via tenacity
"""

import json
import os
import logging
from datetime import datetime

import requests
import websocket
import rel
from kafka import KafkaProducer
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("crypto_producer")

# ── Config from environment ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW          = os.getenv("KAFKA_TOPIC_RAW", "crypto_trades_raw")
TOPIC_DLQ          = os.getenv("KAFKA_TOPIC_DLQ", "crypto_trades_dlq")
BINANCE_REST_URL   = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
BINANCE_WS_URL     = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")
TOP_N              = int(os.getenv("TOP_N_COINS", "50"))

# Required fields to validate a trade tick
REQUIRED_FIELDS = {"e", "E", "s", "t", "p", "q", "T", "m"}


# ── Kafka Producer ─────────────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",                        # wait for full replication ack
        retries=5,
        max_in_flight_requests_per_connection=1,  # preserve ordering
    )


def fetch_top_symbols(n: int = TOP_N) -> list[str]:
    """Call Binance REST /ticker/24hr, sort by quoteVolume, return top-N USDT pairs."""
    log.info("Fetching 24hr tickers from Binance REST API")
    resp = requests.get(
        f"{BINANCE_REST_URL}/api/v3/ticker/24hr",
        timeout=15
    )
    resp.raise_for_status()
    tickers = resp.json()

    # Filter to USDT pairs only for consistency, sort by quoteVolume desc
    usdt_pairs = [t for t in tickers if t["symbol"].endswith("USDT")]
    usdt_pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)

    symbols = [t["symbol"].lower() for t in usdt_pairs[:n]]
    log.info("Top %d symbols selected: %s", n, symbols)
    return symbols


# ── Step 2: Build WebSocket stream URL ────────────────────────────────────────
def build_ws_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s}@trade" for s in symbols)
    url = f"{BINANCE_WS_URL}?streams={streams}"
    log.info("WebSocket URL built with %d streams", len(symbols))
    return url


# ── Step 3: Message handler ───────────────────────────────────────────────────
def make_on_message(producer: KafkaProducer):
    def on_message(ws, raw_message: str):
        try:
            wrapper = json.loads(raw_message)
            # Combined streams wrap payload in {"stream":..., "data":{...}}
            tick = wrapper.get("data", wrapper)

            # Validate required fields
            if not REQUIRED_FIELDS.issubset(tick.keys()):
                raise ValueError(f"Missing fields. Got: {list(tick.keys())}")

            # Enrich with ingestion timestamp
            tick["ingested_at"] = datetime.utcnow().isoformat()

            symbol = tick["s"]  # e.g. BTCUSDT
            producer.send(TOPIC_RAW, key=symbol, value=tick)

        except (json.JSONDecodeError, ValueError) as exc:
            log.warning("Malformed message → DLQ: %s | Error: %s", raw_message[:120], exc)
            producer.send(TOPIC_DLQ, value={"raw": raw_message, "error": str(exc)})

    return on_message


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    log.warning("WebSocket closed: %s %s", close_status_code, close_msg)


def on_open(ws):
    log.info("WebSocket connection established.")


# ── Step 4+5: Connect with auto-reconnect ─────────────────────────────────────
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(20),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def run_stream(producer: KafkaProducer, ws_url: str):
    """Open WebSocket and block. On disconnect, tenacity retries with backoff."""
    log.info("Connecting to WebSocket: %s", ws_url[:80] + "...")
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=make_on_message(producer),
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    # run_forever non-blocking with rel dispatcher to send heartbeats
    ws.run_forever(
        dispatcher=rel,
        ping_interval=60,
        ping_timeout=10
    )
    rel.signal(2, rel.abort)  # Allow safe exit via KeyboardInterrupt
    rel.dispatch()
    
    raise ConnectionError("WebSocket disconnected unexpectedly — triggering retry.")


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=== Crypto Stream Producer starting ===")
    producer = create_producer()

    try:
        symbols  = fetch_top_symbols()
        ws_url   = build_ws_url(symbols)
        run_stream(producer, ws_url)
    except KeyboardInterrupt:
        log.info("Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()
        log.info("Kafka producer closed cleanly.")
