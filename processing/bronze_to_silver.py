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
from delta.tables import DeltaTable

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("bronze_to_silver")

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_PATH      = "gs://crypto-lakehouse-group8/bronze"
SILVER_PATH      = "gs://crypto-lakehouse-group8/silver"
QUARANTINE_PATH  = "gs://crypto-lakehouse-group8/silver/quarantine"
# DEFAULT to cluster URL — scripts must run distributed, not local
SPARK_MASTER     = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# DecimalType(38, 18): 38 total digits, 18 after decimal point.
# This matches the precision used in SQL financial systems and avoids
# the IEEE-754 rounding issue that DoubleType introduces for pairs
# like BTCUSDT where prices can be e.g. 69420.12345678
PRICE_DECIMAL = DecimalType(38, 18)


# ── SparkSession ──────────────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    """Create a SparkSession configured for cluster run with Delta Lake + GCS."""
    log.info("Connecting to Spark Master: %s", SPARK_MASTER)
    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .master(SPARK_MASTER)
        # ── Delta Lake extension ───────────────────────────────────────────
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # ── GCS Connector Auth (Nuclear approach) ─────────────────────
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/home/spark/.config/gcloud/application_default_credentials.json"))
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
        # Delta Atomicity on GCS
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
        # ── Performance tuning for cluster mode ────────────────────────────
        .config("spark.driver.memory",   "1g")
        .config("spark.executor.memory", "1500m")  # match worker mem_limit
        .config("spark.executor.cores",  "2")       # match worker cores
        .config("spark.sql.shuffle.partitions", "8")  # 4 cores total in cluster
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession ready — version %s | master: %s",
             spark.version, spark.sparkContext.master)
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
            F.when(F.col("event_type") != "trade",      F.lit("invalid event_type")),
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
        .withColumn("event_time",  (F.col("event_time_ms") / 1000).cast(TimestampType()))
        .withColumn("trade_time",  (F.col("trade_time") / 1000).cast(TimestampType()))
        # dt = DATE derived from event_time, used for partition pruning
        .withColumn("dt", F.to_date((F.col("event_time_ms") / 1000).cast(TimestampType())))

        # ── Rename raw fields → descriptive Silver column names ────────────
        .withColumnRenamed("s", "symbol")
        .withColumnRenamed("buyer_maker", "buyer_is_maker")

        # ── Audit column ───────────────────────────────────────────────────
        .withColumn("silver_ingested_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))

        # ── Drop superseded raw fields ─────────────────────────────────────
        .drop("p", "q", "event_time_ms", "ignore_m")
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


# ── Silver Writer (UPSERT) ────────────────────────────────────────────────────
def write_silver(spark: SparkSession, df: DataFrame):
    """
    Write to Silver Delta table using MERGE INTO (Upsert).

    Why Merge over Overwrite?
    - Dual ingestion (WS + REST) guarantees overlapping trade_ids.
    - Idempotency is preserved by only inserting new trades (`whenNotMatchedInsertAll`),
      or updating existing if you have mutating logic (we just ignore them here since
      trades are immutable).
    """
    log.info("Writing Silver Delta table to: %s", SILVER_PATH)

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        log.info("Table exists. Performing MERGE (upsert)...")
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_table.alias("target")
            .merge(
                df.alias("source"),
                "target.trade_id = source.trade_id AND target.symbol = source.symbol"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("✅ Silver merge (upsert) complete.")
    else:
        log.info("Table does not exist. Performing initial creation...")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            # FIX 2: two-level partition hierarchy (symbol / dt)
            .partitionBy("symbol", "dt")
            .save(SILVER_PATH)
        )
        log.info("✅ Silver initial write complete — partitioned by (symbol, dt)")


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

        # 6. Write Silver (Merge/Upsert)
        write_silver(spark, deduped_df)

        log.info("=== Bronze → Silver Pipeline COMPLETE ===")

    except Exception as exc:
        # Only genuine infra/Spark failures reach here (not DQ issues)
        log.critical("Infrastructure failure — aborting pipeline: %s", exc, exc_info=True)
        raise  # let the orchestrator (Airflow) handle retry/alerting
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
