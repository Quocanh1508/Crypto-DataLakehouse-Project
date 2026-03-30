"""
producer_batch.py
=================
Historical 1-minute Klines (OHLCV) REST Pull → MinIO raw-batch

Steps:
  1. Re-use the same Top-50 symbol list as the streaming producer
  2. For each symbol, fetch klines from Binance /api/v3/klines
  3. Save as CSV to s3a://raw-batch/history/<SYMBOL>/klines.csv
  4. Respects Binance rate-limit budget (1200 weight/min)
"""

import csv
import io
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import requests
from botocore.client import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("batch_producer")

# ── Config ────────────────────────────────────────────────────────────────────
BINANCE_REST_URL = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
BUCKET_NAME      = "raw-batch"
TOP_N            = int(os.getenv("TOP_N_COINS", "50"))

# Binance klines params
KLINE_INTERVAL   = "1m"    # 1-minute candles
KLINE_LIMIT      = 1000    # max per request (weight = 2)
WEIGHT_PER_REQ   = 2
WEIGHT_LIMIT     = 1100    # stay safely below 1200 limit
SLEEP_PER_SYMBOL = 0.5     # seconds between symbols to avoid bursts

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "num_trades",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
]


# ── MinIO S3 Client ───────────────────────────────────────────────────────────
def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ── Fetch Top-50 (same logic as streaming producer) ───────────────────────────
def fetch_top_symbols(n: int = TOP_N) -> list[str]:
    log.info("Fetching Top-%d symbols from Binance 24hr ticker ...", n)
    resp = requests.get(f"{BINANCE_REST_URL}/api/v3/ticker/24hr", timeout=15)
    resp.raise_for_status()
    tickers = resp.json()
    usdt_pairs = [t for t in tickers if t["symbol"].endswith("USDT")]
    usdt_pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    symbols = [t["symbol"] for t in usdt_pairs[:n]]
    log.info("Symbols: %s", symbols)
    return symbols


# ── Fetch klines for a single symbol ─────────────────────────────────────────
def fetch_klines(symbol: str, interval: str = KLINE_INTERVAL, limit: int = KLINE_LIMIT) -> list[list]:
    url = f"{BINANCE_REST_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ── Convert klines list to CSV bytes ─────────────────────────────────────────
def klines_to_csv_bytes(symbol: str, klines: list[list]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["symbol"] + KLINE_COLUMNS)
    for row in klines:
        writer.writerow([symbol] + row)
    return buffer.getvalue().encode("utf-8")


# ── Upload to MinIO ───────────────────────────────────────────────────────────
def upload_to_minio(s3, symbol: str, csv_bytes: bytes):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"history/{symbol}/{date_str}/klines.csv"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=csv_bytes,
        ContentType="text/csv",
    )
    log.info("Uploaded s3://%s/%s (%d bytes)", BUCKET_NAME, key, len(csv_bytes))


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    log.info("=== Batch Historical Klines Producer starting ===")
    s3 = create_s3_client()
    symbols = fetch_top_symbols()

    total_weight = 0
    for i, symbol in enumerate(symbols, 1):
        try:
            log.info("[%d/%d] Fetching klines for %s ...", i, len(symbols), symbol)
            klines = fetch_klines(symbol)
            csv_bytes = klines_to_csv_bytes(symbol, klines)
            upload_to_minio(s3, symbol, csv_bytes)

            total_weight += WEIGHT_PER_REQ
            if total_weight >= WEIGHT_LIMIT:
                log.warning("Approaching rate limit (%d weight). Sleeping 60s ...", total_weight)
                time.sleep(60)
                total_weight = 0
            else:
                time.sleep(SLEEP_PER_SYMBOL)

        except requests.HTTPError as exc:
            log.error("HTTP error for %s: %s — skipping", symbol, exc)
        except Exception as exc:
            log.error("Unexpected error for %s: %s — skipping", symbol, exc)

    log.info("=== Batch ingestion complete. %d symbols processed. ===", len(symbols))


if __name__ == "__main__":
    run()
