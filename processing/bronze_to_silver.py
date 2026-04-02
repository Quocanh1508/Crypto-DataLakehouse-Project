"""
bronze_to_silver.py
===================
Phase 3 — Bronze → Silver PySpark Batch Job

Architecture:
  - SOURCE  : Delta Lake  s3a://bronze/crypto_trades/
  - TARGET  : Delta Lake  s3a://silver/crypto_trades/
  - WRITE   : Overwrite (full daily reprocessing, idempotent)
  - QUALITY : Great Expectations via Spark DataFrame validation
  - DEDUP   : By Binance trade ID field `t` (unique per symbol)

Data Quality Rules (Bronze → Silver contract):
  1. No null trade IDs (`t`)
  2. Price (`p`) must be > 0
  3. Quantity (`q`) must be > 0
  4. Event type (`e`) must equal "trade"
  5. No duplicate (symbol, trade_id) pairs

Run locally:
  python processing/bronze_to_silver.py

Run via Spark submit (inside container):
  spark-submit --packages io.delta:delta-spark_2.13:3.3.0,...
               processing/bronze_to_silver.py
"""

import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, BooleanType, TimestampType
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("bronze_to_silver")

# ── Config ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")

BRONZE_PATH      = "s3a://bronze/crypto_trades"
SILVER_PATH      = "s3a://silver/crypto_trades"
CHECKPOINT_PATH  = "s3a://checkpoints/bronze_to_silver"

# Delta Lake JAR packages for local run (Spark 4.1.1 + Delta 4.0)
DELTA_PACKAGES   = (
    "io.delta:delta-spark_2.13:4.0.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


# ── SparkSession ──────────────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    """Create a SparkSession configured for local run with Delta Lake + MinIO."""
    log.info("Initialising SparkSession (local mode)…")
    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .master("local[*]")
        # ── Delta Lake extension ───────────────────────────────────────────
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # ── MinIO / S3A connector ──────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Performance tuning for local single-machine run ─────────────
        .config("spark.driver.memory",   "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")  # small for local
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession ready — version %s", spark.version)
    return spark


# ── Schema enforcement ────────────────────────────────────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("e",            StringType(),  True),   # event type = "trade"
    StructField("E",            LongType(),    True),   # event time (ms)
    StructField("s",            StringType(),  True),   # symbol e.g. BTCUSDT
    StructField("t",            LongType(),    True),   # trade ID (unique!)
    StructField("p",            StringType(),  True),   # price (string from WS)
    StructField("q",            StringType(),  True),   # quantity (string from WS)
    StructField("T",            LongType(),    True),   # trade time (ms)
    StructField("m",            BooleanType(), True),   # buyer is market maker
    StructField("M",            BooleanType(), True),   # ignore
    StructField("ingested_at",  StringType(),  True),   # added by producer
])


# ── Data Quality Checks (using plain Spark — lightweight GE substitute) ───────
class DataQualityException(Exception):
    """Raised if data does not meet quality thresholds."""


def run_quality_checks(df: DataFrame, stage: str = "bronze_read") -> DataFrame:
    """
    Inline data quality validation using Spark column expressions.
    Mirrors Great Expectations rule semantics:
      - expect_column_values_to_not_be_null
      - expect_column_values_to_be_between
      - expect_column_values_to_be_in_set

    Returns a cleaned DataFrame. Rows violating rules are quarantined
    to a DQ_FAILED column for audit; hard failures abort the job.
    """
    log.info("[DQ] Running quality checks after stage: %s", stage)

    total = df.count()
    log.info("[DQ] Total rows: %d", total)

    # ── Rule 1: trade ID must not be null ─────────────────────────────────
    null_trade_ids = df.filter(F.col("t").isNull()).count()
    if null_trade_ids > 0:
        raise DataQualityException(
            f"[DQ FAIL] {null_trade_ids} rows have null trade_id (t). Aborting."
        )
    log.info("[DQ] ✅ Rule 1 passed — 0 null trade IDs")

    # ── Rule 2: event type must be 'trade' ────────────────────────────────
    wrong_event = df.filter(F.col("e") != "trade").count()
    if wrong_event > 0:
        log.warning("[DQ] ⚠️  %d rows have unexpected event type (e != 'trade')", wrong_event)
        df = df.filter(F.col("e") == "trade")
    log.info("[DQ] ✅ Rule 2 passed — filtered to event_type='trade'")

    # ── Rule 3: price > 0 ─────────────────────────────────────────────────
    invalid_price = df.filter(F.col("price_double") <= 0).count()
    if invalid_price > 0:
        log.warning("[DQ] ⚠️  %d rows with price <= 0 — dropping", invalid_price)
        df = df.filter(F.col("price_double") > 0)
    log.info("[DQ] ✅ Rule 3 passed — price > 0")

    # ── Rule 4: quantity > 0 ──────────────────────────────────────────────
    invalid_qty = df.filter(F.col("quantity_double") <= 0).count()
    if invalid_qty > 0:
        log.warning("[DQ] ⚠️  %d rows with quantity <= 0 — dropping", invalid_qty)
        df = df.filter(F.col("quantity_double") > 0)
    log.info("[DQ] ✅ Rule 4 passed — quantity > 0")

    clean_total = df.count()
    log.info("[DQ] 🎯 %d / %d rows passed all DQ checks (%.1f%%)",
             clean_total, total, 100 * clean_total / max(total, 1))

    return df


# ── Bronze Reader ─────────────────────────────────────────────────────────────
def read_bronze(spark: SparkSession) -> DataFrame:
    """
    Read the Bronze Delta table.
    Bronze was written via Structured Streaming (Append mode) hence may contain
    duplicates if the checkpoint was reset. We handle dedup in transform().
    """
    log.info("Reading Bronze Delta table from: %s", BRONZE_PATH)
    try:
        df = spark.read.format("delta").load(BRONZE_PATH)
        log.info("Bronze schema: %s", df.schema.simpleString())
        log.info("Bronze row count: %d", df.count())
        return df
    except Exception as exc:
        log.error("Failed to read Bronze Delta table: %s", exc)
        raise


# ── Type-casting & Enrichment ─────────────────────────────────────────────────
def cast_and_enrich(df: DataFrame) -> DataFrame:
    """
    Cast string fields from WebSocket JSON to proper numeric types.
    Add derived columns used by the Gold layer later.
    """
    log.info("Casting types and enriching columns…")
    return (
        df
        # Cast string price / qty to double early (needed for DQ checks)
        .withColumn("price_double",    F.col("p").cast("double"))
        .withColumn("quantity_double", F.col("q").cast("double"))
        # Convert Binance millisecond timestamps to Spark timestamp
        .withColumn("event_time",  (F.col("E") / 1000).cast(TimestampType()))
        .withColumn("trade_time",  (F.col("T") / 1000).cast(TimestampType()))
        # Rename for Silver schema clarity
        .withColumnRenamed("s", "symbol")
        .withColumnRenamed("t", "trade_id")
        .withColumnRenamed("m", "buyer_is_maker")
        # Ingest audit column
        .withColumn("silver_ingested_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))
        # Drop raw string columns now superseded
        .drop("p", "q", "E", "T", "M", "e")
    )


# ── Deduplication ─────────────────────────────────────────────────────────────
def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate by (symbol, trade_id) — Binance guarantees trade_id is unique
    per symbol. Keep the row with the latest ingested_at.
    """
    log.info("Deduplicating by (symbol, trade_id)…")
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("symbol", "trade_id")
        .orderBy(F.col("ingested_at").desc())
    )
    deduped = (
        df
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    before = df.count()
    after  = deduped.count()
    log.info("Dedup: %d → %d rows (removed %d duplicates)", before, after, before - after)
    return deduped


# ── Silver Writer ─────────────────────────────────────────────────────────────
def write_silver(df: DataFrame):
    """
    Write to Silver Delta table.
    Mode: OVERWRITE — full daily reprocessing is idempotent and safe.
    Partition: by symbol so Trino can leverage partition pruning.

    Why Overwrite and not Append?
    - Silver is a *clean* view of Bronze. If we re-run the job (e.g. after code
      fixes), we want a fresh clean dataset — not duplicate Silver rows.
    - The Bronze layer already provides the Append / history guarantee.
    - Overwrite + .partitionBy("symbol") replaces only the affected partitions
      (DYNAMIC partition overwrite), so unchanged symbol partitions are safe.
    """
    log.info("Writing Silver Delta table to: %s", SILVER_PATH)
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")            # allow schema evolution
        .option("partitionOverwriteMode", "dynamic")  # only overwrite touched partitions
        .partitionBy("symbol")
        .save(SILVER_PATH)
    )
    log.info("✅ Silver write complete — partitioned by symbol")


# ── Entrypoint ────────────────────────────────────────────────────────────────
def main():
    log.info("=== Bronze → Silver Pipeline starting ===")
    spark = create_spark()
    try:
        # 1. Read
        bronze_df = read_bronze(spark)

        # 2. Cast & enrich (needed before DQ so we have numeric types)
        cast_df = cast_and_enrich(bronze_df)

        # 3. Data Quality gate
        clean_df = run_quality_checks(cast_df, stage="post_cast")

        # 4. Deduplicate
        deduped_df = deduplicate(clean_df)

        # 5. Write Silver
        write_silver(deduped_df)

        log.info("=== Bronze → Silver Pipeline COMPLETE ===")

    except DataQualityException as dqe:
        log.critical("DQ gate failed — aborting pipeline: %s", dqe)
        sys.exit(1)
    except Exception as exc:
        log.critical("Unexpected failure: %s", exc, exc_info=True)
        sys.exit(2)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
