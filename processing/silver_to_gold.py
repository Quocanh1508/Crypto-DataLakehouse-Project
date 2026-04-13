"""
silver_to_gold.py
=================
Giai đoạn 4 — Silver → Gold Delta Lake (Xử lý batch tổng hợp OHLCV)

Kiến trúc:
  NGUỒN DỮ LIỆU : Delta Lake gs://crypto-lakehouse-group8/silver (dữ liệu tick đã làm sạch)
  ĐÍCH DỮ LIỆU  : Delta Lake gs://crypto-lakehouse-group8/gold (nến OHLCV)
  CHẾ ĐỘ        : Overwrite (xử lý lại toàn bộ dữ liệu theo ngày, có thể chạy lại an toàn)
  TỔNG HỢP      : OHLCV khung 1 phút + OHLCV khung 5 phút
  TÙY CHỌN      : Đường trung bình động (MA7, MA20, MA50 trên giá đóng cửa)
  PHÂN VÙNG     : (symbol, candle_time_date) — giúp truy vấn Trino hiệu quả

Các quyết định thiết kế:
  - OHLCV = Open (giá đầu tiên), High (giá cao nhất), Low (giá thấp nhất), Close (giá cuối cùng), Volume (tổng quantity)
  - Sử dụng window functions trên (symbol, time_bucket) để tổng hợp tick → nến
  - Moving Averages được tính bằng cửa sổ LAG trên giá Close (chu kỳ nhìn lại: 7/20/50 phiên)
  - Partition theo (symbol, ngày của candle_time) để phù hợp với mẫu truy vấn Trino
  - Chế độ ghi = Overwrite (an toàn: job có tính idempotent, có thể chạy lại bất kỳ ngày nào)
  - Delta Lake đảm bảo tính ACID cho các truy vấn đọc đồng thời từ Trino

Logic nghiệp vụ:
  1. Đọc Silver: toàn bộ các dòng (symbol, event_time, price, quantity)
  2. Tạo nến 1m: group by theo (symbol, window(event_time, "1 minute"))
  3. Tạo nến 5m: group by theo (symbol, window(event_time, "5 minutes"))
  4. Tính moving averages trên giá close (tùy chọn, được điều khiển bằng biến môi trường)
  5. Gộp cả hai tập nến vào một bảng Gold duy nhất (hoặc tách riêng nếu cần)
  6. Ghi vào gs://crypto-lakehouse-group8/gold partition theo (symbol, candle_time_date)

Chạy cục bộ:
  python processing/silver_to_gold.py

Chạy bằng Spark submit (bên trong container):
  spark-submit --packages io.delta:delta-spark_2.13:3.1.0,com.google.cloud.bigdataoss:gcs-connector:3.0.0 \
               --conf spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore \
               processing/silver_to_gold.py
"""
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("silver_to_gold")

SILVER_PATH      = "gs://crypto-lakehouse-group8/silver"
GOLD_PATH        = "gs://crypto-lakehouse-group8/gold"

SPARK_MASTER     = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

COMPUTE_MA       = os.getenv("COMPUTE_MOVING_AVERAGES", "true").lower() == "true"
MA_PERIODS       = [7, 20, 50]  

PRICE_DECIMAL = DecimalType(38, 18)


def create_spark() -> SparkSession:
    """Create a SparkSession configured for cluster run with Delta Lake + GCS."""
    log.info("Connecting to Spark Master: %s", SPARK_MASTER)
    spark = (
        SparkSession.builder
        .appName("SilverToGold")
        .master(SPARK_MASTER)
        # NOTE: Delta Lake read/write works without explicit extensions in Spark 3.5.8
        # Delta 3.1.0 is compatible without needing DeltaSparkSessionExtension
        # ── GCS Connector Auth (Application Default Credentials) ─────────
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", 
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/home/spark/.config/gcloud/application_default_credentials.json"))
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
        # Delta Atomicity on GCS
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
        # ── Performance tuning for cluster mode ────────────────────────────
        .config("spark.driver.memory",   "1g")
        .config("spark.executor.memory", "1500m")
        .config("spark.executor.cores",  "2")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession ready — version %s | master: %s",
             spark.version, spark.sparkContext.master)
    return spark


def read_silver(spark: SparkSession) -> DataFrame:
    """
    Read cleansed Silver data from GCS (Parquet files from Delta Lake).
    
    Expected schema (from bronze_to_silver.py):
      - symbol: string
      - event_time: timestamp (UTC)
      - trade_id: long
      - price_decimal: decimal(38, 18)
      - quantity_decimal: decimal(38, 18)
      - ... (other fields)
    """
    log.info("Reading Silver layer from: %s", SILVER_PATH)
    
    # Read Delta Lake format (not plain Parquet)
    df = spark.read.format("delta").load(SILVER_PATH)
    
    # Validate required columns
    required_cols = {"symbol", "event_time", "price_decimal", "quantity_decimal"}
    available_cols = set(df.columns)
    
    if not required_cols.issubset(available_cols):
        missing = required_cols - available_cols
        raise ValueError(f"Silver layer missing columns: {missing}. Found: {available_cols}")
    
    # Cast price and quantity to ensure consistency
    df = (
        df.withColumn("price", F.col("price_decimal").cast("double"))
        .withColumn("quantity", F.col("quantity_decimal").cast("double"))
    )
    
    log.info("Silver table loaded: %d rows", df.count())
    return df


# ── Build OHLCV Candles ───────────────────────────────────────────────────────
def build_ohlcv_candles(df: DataFrame, window_duration: str) -> DataFrame:
    """
    Aggregate ticks into OHLCV candles using time window.
    
    Args:
        df: Silver DataFrame with (symbol, event_time, price, quantity)
        window_duration: "1 minute" or "5 minutes"
    
    Returns:
        DataFrame with columns:
          - symbol
          - candle_time (start of window)
          - open, high, low, close (prices)
          - volume (sum of quantities)
          - tick_count (number of trades in candle)
    """
    log.info("Building %s OHLCV candles...", window_duration)
    
    # Create time window
    windowed = (
        df.withColumn(
            "time_bucket",
            F.window(F.col("event_time"), window_duration)
        )
        .withColumn("candle_time", F.col("time_bucket.start"))
    )
    
    # Aggregate by (symbol, candle_time)
    # Open = first price, High = max, Low = min, Close = last price, Volume = sum qty
    ohlcv = (
        windowed
        .groupBy("symbol", "candle_time")
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("tick_count"),
        )
        .withColumn("candle_duration", F.lit(window_duration))
    )
    
    log.info("Generated %d candles", ohlcv.count())
    return ohlcv


# ── Compute Moving Averages ───────────────────────────────────────────────────
def compute_moving_averages(df: DataFrame, ma_periods: list[int]) -> DataFrame:
    """
    Compute moving averages on close price for each symbol.
    
    Args:
        df: DataFrame with (symbol, candle_time, close, ...)
        ma_periods: list of periods [7, 20, 50]
    
    Returns:
        DataFrame with new columns: ma_7, ma_20, ma_50 (nullable if insufficient history)
    """
    log.info("Computing moving averages: %s", ma_periods)
    
    # Define window: order by candle_time, partition by symbol
    # Use ROWS frame instead of RANGE to avoid TIMESTAMP type mismatch
    symbol_window = Window.partitionBy("symbol").orderBy("candle_time")
    
    result = df
    for period in ma_periods:
        ma_col_name = f"ma_{period}"
        result = (
            result.withColumn(
                ma_col_name,
                F.avg("close").over(
                    symbol_window.rowsBetween(-(period - 1), 0)
                )
            )
        )
    
    log.info("Moving averages computed")
    return result


# ── Merge & Prepare for Gold ──────────────────────────────────────────────────
def prepare_gold_table(df_1m: DataFrame, df_5m: DataFrame) -> DataFrame:
    """
    Combine 1-minute and 5-minute candles into a single Gold table.
    
    Strategy:
      1. Add a marker column to identify candle type
      2. Union both datasets
      3. Add metadata (processing_timestamp, candle_date for partitioning)
    
    Args:
        df_1m: 1-minute OHLCV candles
        df_5m: 5-minute OHLCV candles
    
    Returns:
        Unified Gold DataFrame ready for write
    """
    log.info("Preparing unified Gold table...")
    
    # Add candle_date for partitioning (date of the candle_time)
    df_1m = df_1m.withColumn("candle_date", F.to_date("candle_time"))
    df_5m = df_5m.withColumn("candle_date", F.to_date("candle_time"))
    
    # Add processing timestamp (when this job ran)
    processing_ts = F.lit(datetime.now(timezone.utc).isoformat())
    df_1m = df_1m.withColumn("processing_timestamp", processing_ts)
    df_5m = df_5m.withColumn("processing_timestamp", processing_ts)
    
    # Union (keep both 1m and 5m in same table, distinguished by candle_duration)
    gold_df = df_1m.unionByName(df_5m, allowMissingColumns=True)
    
    # Reorder columns for clarity
    column_order = [
        "symbol",
        "candle_time",
        "candle_date",
        "candle_duration",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "tick_count",
    ]
    
    # Add MA columns if computed
    if COMPUTE_MA:
        column_order.extend([f"ma_{p}" for p in MA_PERIODS])
    
    column_order.extend(["processing_timestamp"])
    
    gold_df = gold_df.select(column_order)
    
    log.info("Gold table prepared: %d rows", gold_df.count())
    return gold_df


# ── Write to Gold Layer ───────────────────────────────────────────────────────
def write_gold(df: DataFrame, gold_path: str) -> None:
    """
    Write Gold table to GCS as Parquet files with partitioning.
    
    Partitioning: (symbol, candle_date)
      - symbol: enables per-symbol queries for dbt tests
      - candle_date: enables date range queries for Time Series analysis
    
    Write mode: Overwrite
      - Safe because this job is idempotent (can rerun any day)
      - Full table recompute from Silver ensures consistency
    """
    log.info("Writing Gold table to: %s (partitioned by symbol, candle_date)", gold_path)
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("symbol", "candle_date")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )
    
    log.info("✅ Gold table written successfully")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 80)
    log.info("PHASE 4: Silver → Gold OHLCV Aggregation")
    log.info("=" * 80)
    log.info("Config: COMPUTE_MA=%s, MA_PERIODS=%s", COMPUTE_MA, MA_PERIODS)
    
    spark = create_spark()
    
    try:
        # 1. Read Silver
        df_silver = read_silver(spark)
        
        # 2. Build 1-minute candles
        df_1m = build_ohlcv_candles(df_silver, "1 minute")
        
        # 3. Build 5-minute candles
        df_5m = build_ohlcv_candles(df_silver, "5 minutes")
        
        # 4. Compute moving averages (optional)
        if COMPUTE_MA:
            df_1m = compute_moving_averages(df_1m, MA_PERIODS)
            df_5m = compute_moving_averages(df_5m, MA_PERIODS)
        
        # 5. Prepare unified Gold table
        df_gold = prepare_gold_table(df_1m, df_5m)
        
        # 6. Write to Gold
        write_gold(df_gold, GOLD_PATH)
        
        log.info("=" * 80)
        log.info("✅ Phase 4 completed successfully!")
        log.info("=" * 80)
        
    except Exception as e:
        log.error("❌ Phase 4 failed: %s", str(e), exc_info=True)
        raise
    
    finally:
        spark.stop()
        log.info("SparkSession closed.")


if __name__ == "__main__":
    main()
