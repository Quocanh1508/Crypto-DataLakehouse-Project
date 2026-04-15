-- =============================================================================
-- mart_crypto_gcs.sql
-- =============================================================================
-- LAYER: Marts (VIEW materialization)
-- SOURCE: source('gold_gcs', 'gold_ohlcv')  ==  delta_gcs.gcs.gold_ohlcv
--         → 17,644 REAL rows from gs://crypto-lakehouse-group8/gold/
-- TARGET: Power BI Desktop connects via Trino ODBC
--
-- HOW TO USE IN POWER BI:
--   Server:   localhost:8080
--   Catalog:  delta
--   Schema:   default_marts
--   Table:    mart_crypto_gcs
--
-- METRICS COMPUTED (same as mart_crypto_dashboard):
--   VWAP             = cumulative intraday Volume Weighted Average Price
--   price_change_pct = (close - prev_close) / prev_close * 100%
--   candle_direction = BULLISH (close >= open) | BEARISH
-- =============================================================================

{{
  config(
    materialized = 'view',
    schema       = 'marts'
  )
}}

WITH src AS (
    -- Read directly from the REAL GCS Gold table (17k+ rows)
    SELECT
        symbol,
        CAST(candle_time     AS TIMESTAMP(3))  AS candle_time,
        CAST(candle_date     AS DATE)           AS trade_date,
        candle_duration,
        CASE
            WHEN candle_duration = '1 minute'  THEN 1
            WHEN candle_duration = '5 minutes' THEN 5
            ELSE NULL
        END                                     AS window_minutes,
        CAST(open   AS DOUBLE)                  AS open,
        CAST(high   AS DOUBLE)                  AS high,
        CAST(low    AS DOUBLE)                  AS low,
        CAST(close  AS DOUBLE)                  AS close,
        CAST(volume AS DOUBLE)                  AS volume,
        CAST(tick_count AS BIGINT)              AS tick_count,
        CAST(ma_7   AS DOUBLE)                  AS ma_7,
        CAST(ma_20  AS DOUBLE)                  AS ma_20,
        CAST(ma_50  AS DOUBLE)                  AS ma_50,
        -- Derived base metrics
        CAST(high AS DOUBLE) - CAST(low AS DOUBLE)                      AS candle_range,
        (CAST(high AS DOUBLE) + CAST(low AS DOUBLE) + CAST(close AS DOUBLE)) / 3.0  AS typical_price,
        candle_time + INTERVAL '1' MINUTE                               AS window_end
    FROM {{ source('gold_gcs', 'gold_ohlcv') }}
    WHERE candle_time <= CURRENT_TIMESTAMP          -- exclude corrupted future rows
),

-- =========================================================================
-- CTE 1: Cumulative VWAP (Volume Weighted Average Price) within each day
-- =========================================================================
vwap_calc AS (
    SELECT
        symbol,
        trade_date,
        window_minutes,
        candle_time,
        SUM(typical_price * volume) OVER (
            PARTITION BY symbol, trade_date, window_minutes
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        /
        NULLIF(
            SUM(volume) OVER (
                PARTITION BY symbol, trade_date, window_minutes
                ORDER BY candle_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        )                                   AS vwap_cumulative
    FROM src
),

-- =========================================================================
-- CTE 2: Price change % vs previous candle (per symbol + window)
-- =========================================================================
price_change AS (
    SELECT
        symbol,
        window_minutes,
        candle_time,
        close,
        LAG(close, 1) OVER (
            PARTITION BY symbol, window_minutes
            ORDER BY candle_time
        )                                   AS prev_close
    FROM src
),

-- =========================================================================
-- CTE 3: Final assembly with all derived metrics
-- =========================================================================
final AS (
    SELECT
        -- Identifiers
        src.symbol,
        src.candle_time,
        src.window_end,
        src.candle_duration,
        src.window_minutes,
        src.trade_date,

        -- OHLCV
        src.open,
        src.high,
        src.low,
        src.close,
        src.volume,
        src.tick_count,

        -- Moving Averages (computed in Gold layer by silver_to_gold.py)
        src.ma_7,
        src.ma_20,
        src.ma_50,

        -- Derived metrics
        src.candle_range,
        src.typical_price,

        -- VWAP cumulative intraday
        vwap_calc.vwap_cumulative,

        -- Price change % vs previous candle
        CASE
            WHEN pc.prev_close IS NULL OR pc.prev_close = 0.0
                THEN NULL
            ELSE
                ROUND(
                    (src.close - pc.prev_close) / pc.prev_close * 100.0,
                    4
                )
        END                                 AS price_change_pct,

        -- Candle direction for Power BI conditional formatting
        CASE
            WHEN src.close >= src.open THEN 'BULLISH'
            ELSE                            'BEARISH'
        END                                 AS candle_direction

    FROM src
    LEFT JOIN vwap_calc
        ON  src.symbol         = vwap_calc.symbol
        AND src.trade_date     = vwap_calc.trade_date
        AND src.window_minutes = vwap_calc.window_minutes
        AND src.candle_time    = vwap_calc.candle_time
    LEFT JOIN price_change pc
        ON  src.symbol         = pc.symbol
        AND src.window_minutes = pc.window_minutes
        AND src.candle_time    = pc.candle_time
)

SELECT * FROM final
ORDER BY symbol, window_minutes, candle_time DESC
