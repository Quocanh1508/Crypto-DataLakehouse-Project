"""
tests/validate_pipeline.py
==========================
Comprehensive automated data quality testing for the Crypto Data Lakehouse.

Tests:
1. Data Integrity: Sums all Kafka topic offsets and compares to Bronze rows.
2. Latency Check:  Calculates the lag between event generation and Kafka ingestion across Bronze.
3. Precision:      Asserts Silver prices (e.g. PEPEUSDT) are DecimalType and rendered correctly.
4. Deduplication:  Asserts 100% uniqueness of (symbol, trade_id) in Silver.
"""

import os
import sys
import logging
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'processing'))
from gcs_auth import apply_gcs_auth

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]  %(message)s")
log = logging.getLogger("validator")

# Config
# NOTE: Default to Docker-internal address (kafka:29092) because this script
# runs inside the Docker lakehouse-net via the Spark cluster.
# If running directly on host (dev/debug), override with:
#   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC           = os.getenv("KAFKA_TOPIC_RAW", "crypto_trades_raw")
# DEFAULT to cluster URL — scripts must run distributed, not local
SPARK_MASTER    = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
BRONZE_PATH     = "gs://crypto-lakehouse-group8/bronze"
SILVER_PATH     = "gs://crypto-lakehouse-group8/silver"



def create_spark() -> SparkSession:
    log.info("Connecting to Spark Master: %s", SPARK_MASTER)
    builder = (
        SparkSession.builder
        .appName("PipelineValidator")  # shows up on Spark UI
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
    )
    # Auto-detect SA Key vs ADC — no hardcoded auth type needed
    builder = apply_gcs_auth(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def check_data_integrity(spark: SparkSession):
    log.info("=== 1. Data Integrity & Loss Test ===")
    
    # 1. Count exact messages published to Kafka using PySpark
    log.info("Querying Kafka broker for total published messages via PySpark...")
    try:
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        kafka_total = kafka_df.count()
    except Exception as e:
        log.error(f"Failed to read from Kafka via Spark: {e}")
        kafka_total = -1

    log.info(f"Total messages strictly generated to Kafka: {kafka_total:,}")

    # 2. Count exact rows in Bronze
    log.info("Querying GCS Bronze layer for persisted rows...")
    try:
        bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
        log.info(f"Total rows persisted in Bronze Delta Lake: {bronze_count:,}")
    except Exception as e:
        log.error(f"Failed to read Bronze Delta table. Does it exist yet? {e}")
        bronze_count = 0

    if kafka_total == -1:
        log.warning("Could not complete comparison due to Kafka connection error.")
    elif bronze_count >= kafka_total:
        log.info("✅ PASS: Bronze row count is complete. ZERO DATA LOSS.")
    else:
        diff = kafka_total - bronze_count
        log.error(f"❌ FAIL: Bronze is missing {diff:,} rows from Kafka stream. Check streaming logs (OOM / Lag).")


def check_latency(spark: SparkSession):
    log.info("\n=== 2. Real-Time Latency Test ===")
    log.info("Computing processing_time vs event_time latency distribution...")
    try:
        df = spark.read.format("delta").load(BRONZE_PATH)
        
        # event time (event_time_ms) is epoch ms. ingested_at is ISO string
        latency_df = (
            df
            .withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("event_time_ms") / 1000)))
            .withColumn("ingest_ts", F.to_timestamp("ingested_at"))
            .withColumn("latency_seconds", F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts"))
        )

        stats = latency_df.select(
            F.avg("latency_seconds").alias("avg_latency"),
            F.max("latency_seconds").alias("max_latency"),
            F.percentile_approx("latency_seconds", 0.95).alias("p95_latency")
        ).collect()[0]

        log.info(f"Average Latency : {stats['avg_latency']:.2f} seconds")
        log.info(f"P95 Latency     : {stats['p95_latency']:.2f} seconds")
        log.info(f"Max Latency     : {stats['max_latency']} seconds")

        if stats['max_latency'] and stats['max_latency'] > 30:
            log.warning("⚠️ High Latency Detected: Max latency > 30s! Recommend scaling spark.sql.shuffle.partitions or allocating more RAM.")
        elif stats['max_latency']:
            log.info("✅ PASS: System is operating smoothly in real-time under the 30s threshold.")
    except Exception as e:
        log.error(f"Failed to process Latency check: {e}")


def check_precision(spark: SparkSession):
    log.info("\n=== 3. Decimal Precision Test ===")
    log.info("Verifying Small-Cap Token (PEPEUSDT / SHIBUSDT) precision adherence...")
    try:
        df = spark.read.format("delta").load(SILVER_PATH)
        
        target_coins = ["pepeusdt", "shibusdt", "PEPEUSDT", "SHIBUSDT"]
        meme_df = df.filter(F.col("symbol").isin(target_coins))
        
        if meme_df.isEmpty():
            log.warning("No PEPEUSDT or SHIBUSDT found in Silver layer yet to explicitly test meme precision. Proceeding loosely.")
            return

        price_dtype = dict(meme_df.dtypes)["price_decimal"]
        log.info(f"Silver price_decimal data type is currently: {price_dtype}")
        
        if "DecimalType" in price_dtype or "decimal(38,18)" in price_dtype:
            log.info("✅ PASS: Financial Decimal(38,18) data type applied. No IEEE rounding possible.")
        else:
            log.error(f"❌ FAIL: Double type remains in use ({price_dtype}). Floating point corruption imminent.")

        log.info("Visual sample of tiny fractionals:")
        meme_df.select("symbol", "trade_id", "price_decimal", "quantity_decimal").show(5, False)
        
    except Exception as e:
        log.error(f"Failed to process Precision check: {e}")


def check_deduplication(spark: SparkSession):
    log.info("\n=== 4. Deduplication & Idempotency Test ===")
    log.info("Executing comprehensive uniqueness check across entire Silver data warehouse...")
    try:
        df = spark.read.format("delta").load(SILVER_PATH)
        
        total_rows = df.count()
        distinct_rows = df.dropDuplicates(["symbol", "trade_id"]).count()
        duplicate_count = total_rows - distinct_rows

        log.info(f"Total Rows Evaluated   : {total_rows:,}")
        log.info(f"Unique Distinct Rows   : {distinct_rows:,}")
        log.info(f"Duplicate Violoations  : {duplicate_count:,}")

        if duplicate_count == 0:
            log.info("✅ PASS: Zero duplicates detected. Silver layer MERGE Upsert logic is bulletproof.")
        else:
            log.error(f"❌ FAIL: {duplicate_count:,} duplicate pairs found. Dual-ingestion Merge logic failed.")
            
    except Exception as e:
        log.error(f"Failed to process Deduplication check: {e}")


def main():
    log.info("Starting automated test framework...")
    spark = create_spark()
    
    check_data_integrity(spark)
    check_latency(spark)
    check_precision(spark)
    check_deduplication(spark)

    spark.stop()
    log.info("\n=== All Automated Tests Completed ===")


if __name__ == "__main__":
    main()
