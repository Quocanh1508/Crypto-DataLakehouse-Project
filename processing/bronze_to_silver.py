"""
bronze_to_silver.py
===================
Phase 3 — Bronze → Silver PySpark Batch Job

Architecture:
  - SOURCE     : Delta Lake  s3a://bronze/crypto_trades/
  - TARGET     : Delta Lake  s3a://silver/crypto_trades/
  - QUARANTINE : Delta Lake  s3a://silver/quarantine/
  - WRITE      : Overwrite (full daily reprocessing, idempotent)
  - QUALITY    : Inline Spark DQ rules (Great Expectations semantics)
  - DEDUP      : By Binance trade ID field `t` (unique per symbol)

Data Quality Rules (Bronze → Silver contract):
  1. No null trade IDs (`t`) → quarantine
  2. Price (`p`) must be > 0 → quarantine
  3. Quantity (`q`) must be > 0 → quarantine
  4. Event type (`e`) must equal "trade" → quarantine
  5. No duplicate (symbol, trade_id) pairs → deduplicate, keep latest

Precision:
  - Price and quantity use DecimalType(38, 18) to avoid IEEE-754 float rounding.

Partitioning:
  - Silver table partitioned by (symbol, dt) where dt = DATE(event_time).
  - Enables daily partition pruning for historical queries in Trino.

Quarantine:
  - Bad rows are NEVER dropped silently. They are written to s3a://silver/quarantine/
    with an extra `quarantine_reason` column for audit and reprocessing.
  - The job NEVER halts due to bad source data — only genuine infrastructure
    failures (S3 unreachable, Spark OOM, etc.) will abort the run.

Run locally:
  python processing/bronze_to_silver.py

Run via Spark submit (inside container):
  spark-submit --packages io.delta:delta-spark_2.13:4.0.0,...
               processing/bronze_to_silver.py
"""

import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, BooleanType, TimestampType, DecimalType
)
from pyspark.sql.window import Window

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
QUARANTINE_PATH  = "s3a://silver/quarantine"

# DecimalType(38, 18): 38 total digits, 18 after decimal point.
# This matches the precision used in SQL financial systems and avoids
# the IEEE-754 rounding issue that DoubleType introduces for pairs
# like BTCUSDT where prices can be e.g. 69420.12345678
PRICE_DECIMAL = DecimalType(38, 18)


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
        # ── Performance tuning for local single-machine run ────────────────
        .config("spark.driver.memory",   "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")  # small for local
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession ready — version %s", spark.version)
    return spark


# ── Data Quality & Quarantine ─────────────────────────────────────────────────
def split_valid_quarantine(df: DataFrame):
    """
    Evaluate each DQ rule and tag bad rows with a quarantine_reason string.
    Returns two DataFrames: (clean_df, quarantine_df).

    Design principle:
      - The job NEVER halts due to bad source data.
      - Bad rows go to s3a://silver/quarantine/ for audit and reprocessing.
      - Only hard infrastructure errors (S3 down, OOM) will abort.
    """
    log.info("[DQ] Evaluating data quality rules…")

    # Build a 'quarantine_reason' column combining all rule violations.
    # An empty string means the row is clean.
    df = df.withColumn(
        "quarantine_reason",
        F.concat_ws("; ",
            F.when(F.col("trade_id").isNull(),          F.lit("null trade_id")),
            F.when(F.col("e") != "trade",               F.lit("invalid event_type")),
            F.when(F.col("price_decimal").isNull(),     F.lit("null price after cast")),
            F.when(F.col("price_decimal") <= 0,         F.lit("price <= 0")),
            F.when(F.col("quantity_decimal").isNull(),  F.lit("null quantity after cast")),
            F.when(F.col("quantity_decimal") <= 0,      F.lit("quantity <= 0")),
        )
    )

    # cache so we don't recompute for both splits
    df.cache()

    clean_df      = df.filter(F.col("quarantine_reason") == "").drop("quarantine_reason")
    quarantine_df = df.filter(F.col("quarantine_reason") != "")

    total      = df.count()
    n_clean    = clean_df.count()
    n_bad      = quarantine_df.count()

    log.info("[DQ] Total: %d | Clean: %d | Quarantine: %d (%.1f%%)",
             total, n_clean, n_bad, 100 * n_bad / max(total, 1))

    return clean_df, quarantine_df


# ── Bronze Reader ─────────────────────────────────────────────────────────────
def read_bronze(spark: SparkSession) -> DataFrame:
    """
    Read the Bronze Delta table (written as Append by bronze_streaming.py).
    May contain duplicates if the streaming checkpoint was ever reset.
    """
    log.info("Reading Bronze Delta table from: %s", BRONZE_PATH)
    df = spark.read.format("delta").load(BRONZE_PATH)
    log.info("Bronze row count: %d", df.count())
    return df


# ── Type-casting & Enrichment ─────────────────────────────────────────────────
def cast_and_enrich(df: DataFrame) -> DataFrame:
    """
    1. Cast price/quantity strings → DecimalType(38,18) — no float rounding.
    2. Convert Binance epoch-ms fields → TimestampType.
    3. Extract `dt` (DateType) from event_time for daily partition column.
    4. Rename raw WS field names (single letters) to human-readable names.
    """
    log.info("Casting types and enriching columns…")
    return (
        df
        # ── FIX 1: Decimal precision — no IEEE-754 rounding ───────────────
        .withColumn("price_decimal",    F.col("p").cast(PRICE_DECIMAL))
        .withColumn("quantity_decimal", F.col("q").cast(PRICE_DECIMAL))

        # ── FIX 2: Timestamps + daily partition column ─────────────────────
        .withColumn("event_time",  (F.col("E") / 1000).cast(TimestampType()))
        .withColumn("trade_time",  (F.col("T") / 1000).cast(TimestampType()))
        # dt = DATE derived from event_time (E), used for partition pruning
        .withColumn("dt", F.to_date((F.col("E") / 1000).cast(TimestampType())))

        # ── Rename raw fields → descriptive Silver column names ────────────
        .withColumnRenamed("s", "symbol")
        .withColumnRenamed("t", "trade_id")
        .withColumnRenamed("m", "buyer_is_maker")

        # ── Audit column ───────────────────────────────────────────────────
        .withColumn("silver_ingested_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))

        # ── Drop superseded raw fields ─────────────────────────────────────
        .drop("p", "q", "E", "T", "M", "e")
    )


# ── Deduplication ─────────────────────────────────────────────────────────────
def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate by (symbol, trade_id) — Binance guarantees trade_id is globally
    unique per symbol. Keep the row with the latest ingested_at timestamp.
    """
    log.info("Deduplicating by (symbol, trade_id)…")
    window = (
        Window.partitionBy("symbol", "trade_id")
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


# ── Quarantine Writer ─────────────────────────────────────────────────────────
def write_quarantine(df: DataFrame):
    """
    Write bad rows to quarantine Delta table for audit and reprocessing.
    Uses Append mode so each run's bad rows accumulate for investigation.
    """
    if df.isEmpty():
        log.info("[Quarantine] No bad rows — skipping quarantine write.")
        return

    log.info("[Quarantine] Writing %d bad rows to: %s", df.count(), QUARANTINE_PATH)
    (
        df
        .withColumn("quarantine_dt", F.current_date())
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("quarantine_dt")
        .save(QUARANTINE_PATH)
    )
    log.info("[Quarantine] ✅ Quarantine write complete.")


# ── Silver Writer ─────────────────────────────────────────────────────────────
def write_silver(df: DataFrame):
    """
    Write to Silver Delta table.

    Mode: OVERWRITE with DYNAMIC partition overwrite.
      - Silver is an idempotent clean view of Bronze.
      - Re-running produces a fresh clean dataset without duplicates.
      - partitionOverwriteMode=dynamic only replaces (symbol, dt) partitions
        that appear in this batch — historical partitions are untouched.

    Partition: (symbol, dt)
      - symbol  → partition pruning when querying a single coin (e.g. BTCUSDT)
      - dt      → FIX 2: partition pruning for historical date-range queries
    """
    log.info("Writing Silver Delta table to: %s", SILVER_PATH)
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("partitionOverwriteMode", "dynamic")
        # FIX 2: two-level partition hierarchy (symbol / dt)
        .partitionBy("symbol", "dt")
        .save(SILVER_PATH)
    )
    log.info("✅ Silver write complete — partitioned by (symbol, dt)")


# ── Entrypoint ────────────────────────────────────────────────────────────────
def main():
    log.info("=== Bronze → Silver Pipeline starting ===")
    spark = create_spark()
    try:
        # 1. Read Bronze
        bronze_df = read_bronze(spark)

        # 2. Cast & enrich (Decimal precision + dt extraction)
        cast_df = cast_and_enrich(bronze_df)

        # 3. FIX 3: Split clean vs bad rows — never abort for bad data
        clean_df, quarantine_df = split_valid_quarantine(cast_df)

        # 4. Write quarantine for audit (always, even if empty)
        write_quarantine(quarantine_df)

        # 5. Deduplicate clean rows
        deduped_df = deduplicate(clean_df)

        # 6. Write Silver
        write_silver(deduped_df)

        log.info("=== Bronze → Silver Pipeline COMPLETE ===")

    except Exception as exc:
        # Only genuine infra/Spark failures reach here (not DQ issues)
        log.critical("Infrastructure failure — aborting pipeline: %s", exc, exc_info=True)
        raise  # let the orchestrator (Airflow) handle retry/alerting
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
