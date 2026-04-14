-- =============================================================================
-- stg_gold_ohlcv.sql
-- =============================================================================
-- LAYER: Staging (materialized as VIEW)
-- SOURCE: {{ source('gold', 'gold_ohlcv') }} = delta.default.gold_ohlcv on Trino
-- DOWNSTREAM: mart_crypto_dashboard.sql reads from this model
--
-- PURPOSE:
--   1. Standardize data types (cast Decimal -> Double for Trino window functions)
--   2. Derive integer window_minutes from string candle_duration ("1 minute" -> 1)
--   3. Compute helper columns: candle_range, typical_price, window_end
--   4. Filter out anomalous future timestamps
--   5. Pass through candle_date for partition pruning in Power BI
--
-- WHY VIEW instead of TABLE?
--   Staging is an intermediate cleanup step. VIEW = no GCS storage cost,
--   Trino computes on-the-fly when queried. dbt run is faster (no Parquet write).
--
-- COLUMN MAPPING (silver_to_gold.py -> staging):
--   candle_time      -> candle_time (passthrough)
--   candle_date      -> trade_date  (alias for BI consistency)
--   candle_duration  -> candle_duration + window_minutes (derived INT)
--   tick_count       -> tick_count (passthrough)
--   ma_7/ma_20/ma_50 -> passthrough
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('gold', 'gold_ohlcv') }}
),

enriched AS (
    SELECT
        -- === Identifiers =====================================================
        symbol,
        candle_time,
        candle_duration,

        -- Derive integer minutes from string duration for calculations
        -- silver_to_gold.py writes: "1 minute" or "5 minutes"
        CASE candle_duration
            WHEN '1 minute'  THEN 1
            WHEN '5 minutes' THEN 5
            ELSE NULL
        END                                              AS window_minutes,

        -- === OHLCV Core ======================================================
        -- Cast to DOUBLE for Trino window functions (VWAP, SMA).
        -- Gold layer stores as DOUBLE from PySpark, but explicit cast ensures safety.
        CAST(open   AS DOUBLE) AS open,
        CAST(high   AS DOUBLE) AS high,
        CAST(low    AS DOUBLE) AS low,
        CAST(close  AS DOUBLE) AS close,
        CAST(volume AS DOUBLE) AS volume,

        -- === Moving Averages (from Gold layer, passthrough) ==================
        ma_7,
        ma_20,
        ma_50,

        -- === Trade Metadata ==================================================
        tick_count,

        -- === Derived: Candle Range ===========================================
        -- Meaning: price range within a candle (high - low)
        -- Used in Power BI to visualize volatility.
        CAST(high AS DOUBLE) - CAST(low AS DOUBLE)       AS candle_range,

        -- === Derived: Typical Price ==========================================
        -- Formula: (high + low + close) / 3
        -- Used to calculate VWAP in the mart layer.
        (CAST(high AS DOUBLE) + CAST(low AS DOUBLE) + CAST(close AS DOUBLE)) / 3.0
                                                         AS typical_price,

        -- === Derived: Window End Time ========================================
        -- Compute end time from candle_time + duration
        CASE candle_duration
            WHEN '1 minute'  THEN candle_time + INTERVAL '1' MINUTE
            WHEN '5 minutes' THEN candle_time + INTERVAL '5' MINUTE
            ELSE NULL
        END                                              AS window_end,

        -- === Partition: Trade Date ===========================================
        -- Use candle_date from Gold directly (already a DATE, computed by PySpark).
        -- Enables Trino partition pruning when Power BI filters by date.
        candle_date                                      AS trade_date,

        -- === Metadata ========================================================
        processing_timestamp

    FROM source
    WHERE
        -- Filter anomalous data: candle_time must not be in the future.
        -- Allow 10-minute clock skew buffer (timezone diff between Binance and GCS).
        candle_time <= (NOW() + INTERVAL '10' MINUTE)
)

SELECT * FROM enriched
