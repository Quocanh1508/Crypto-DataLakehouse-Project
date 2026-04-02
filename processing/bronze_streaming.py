"""
bronze_streaming.py
===================
Phase 3 — Kafka → Bronze Delta Lake (Structured Streaming)

Architecture:
  SOURCE     : Kafka topic `crypto_trades_raw` (JSON)
  SINK       : Delta Lake s3a://bronze/crypto_trades/
  MODE       : Append (streaming, raw/immutable)
  TRIGGER    : ProcessingTime("30 seconds") — micro-batch every 30s
  PARTITIONS : (processing_date, s) — date-first prevents small files
  CHECKPOINT : s3a://checkpoints/kafka_to_bronze/

Design decisions:
  - NO deduplication at Bronze. Bronze is raw truth. Any dedup logic here
    risks permanent data loss if the logic is wrong. Silver owns cleansing.
  - NO withWatermark+dropDuplicates in streaming. Without a watermark,
    Spark holds ALL historical state in memory → OOM on long runs.
  - Partition by (processing_date, s): date-first partitioning creates
    daily directory boundaries per symbol, preventing millions of tiny
    files from accumulating in a single symbol partition over months.

Run locally:
  python processing/bronze_streaming.py
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, BooleanType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("kafka_to_bronze")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS",  "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC_RAW",          "crypto_trades_raw")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",           "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",         "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",         "admin123")

BRONZE_PATH      = "s3a://bronze/crypto_trades"
CHECKPOINT_PATH  = "s3a://checkpoints/kafka_to_bronze"

# ── Known schema for Binance trade tick ──────────────────────────────────────
TRADE_SCHEMA = StructType([
    StructField("e",           StringType(),  True),
    StructField("E",           LongType(),    True),
    StructField("s",           StringType(),  True),
    StructField("t",           LongType(),    True),
    StructField("p",           StringType(),  True),
    StructField("q",           StringType(),  True),
    StructField("T",           LongType(),    True),
    StructField("m",           BooleanType(), True),
    StructField("M",           BooleanType(), True),
    StructField("ingested_at", StringType(),  True),
])


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("KafkaToBronze")
        .master("local[2]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.memory",   "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    log.info("=== Kafka → Bronze Streaming job starting ===")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ───────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "earliest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    # ── Deserialise JSON payload ──────────────────────────────────────────
    parsed = (
        raw_stream
        .select(
            F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("data")
        )
        .select("data.*")
        # FIX 1 & 2: No deduplication here.
        # Bronze = raw immutable truth. Dedup belongs in Silver batch job.
        # Streaming dedup without withWatermark holds ALL history in memory → OOM.
        # FIX 3: Derive processing_date from Binance event time field E (epoch ms).
        # Date-first partitioning creates daily boundaries, preventing millions of
        # tiny files from accumulating in a single symbol directory over months.
        .withColumn(
            "processing_date",
            F.to_date(F.from_unixtime(F.col("E") / 1000))
        )
    )

    # ── Write to Bronze Delta (Append, raw, no filtering) ────────────────
    # Why Append?
    #   - Bronze is raw/immutable history. We never overwrite it.
    #   - Streaming requires Append or Complete mode.
    # Why (processing_date, s) partition order?
    #   - Date-first means each micro-batch only opens ONE directory per day.
    #   - Symbol-first would open 50 directories per micro-batch, creating
    #     O(symbols × batches-per-day) = O(50 × 2880) = 144,000 tiny files/day.
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")              # handle field additions gracefully
        .partitionBy("processing_date", "s")        # FIX 3: date-first partition
        .trigger(processingTime="30 seconds")
        .start(BRONZE_PATH)
    )

    log.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
