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

BRONZE_PATH      = "gs://crypto-lakehouse-group8/bronze"
CHECKPOINT_PATH  = "gs://crypto-lakehouse-group8/checkpoints/kafka_to_bronze"

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
        # ── GCS Connector / ADC Auth ───────────────────────────────────────
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "false")
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT")
        # Delta Atomicity on GCS
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
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
