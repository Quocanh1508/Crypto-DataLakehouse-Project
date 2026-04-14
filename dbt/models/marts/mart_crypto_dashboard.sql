-- =============================================================================
-- mart_crypto_dashboard.sql
-- =============================================================================
-- LAYER: Marts (materialized as VIEW for local dev; use TABLE on HDFS/GCS)
-- SOURCE: {{ ref('stg_gold_ohlcv') }} = staging view
-- TARGET: Power BI Desktop connects to this view via Trino ODBC
--
-- PURPOSE:
--   1. Compute advanced analytics: VWAP (cumulative intraday), price_change_pct
--   2. Add candle_direction label (BULLISH/BEARISH) for conditional formatting
--   3. Materialize as VIEW in Trino (Delta Lake on S3 does not support RENAME)
--   4. Limit to last 30 days to avoid full scan
--
-- METRICS COMPUTED:
--   VWAP = Sum(typical_price * volume) / Sum(volume) cumulative intraday
--   price_change_pct = (close - prev_close) / prev_close * 100%
--   candle_direction = BULLISH if close >= open, else BEARISH
-- =============================================================================

WITH stg AS (
    SELECT * FROM {{ ref('stg_gold_ohlcv') }}
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
        )                                 AS vwap_cumulative
    FROM stg
),

-- =========================================================================
-- CTE 2: Price change % vs previous candle
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
        )                                 AS prev_close
    FROM stg
),

-- =========================================================================
-- CTE 3: Final assembly
-- =========================================================================
final AS (
    SELECT
        -- Identifiers
        stg.symbol,
        stg.candle_time,
        stg.window_end,
        stg.candle_duration,
        stg.window_minutes,
        stg.trade_date,

        -- OHLCV
        stg.open,
        stg.high,
        stg.low,
        stg.close,
        stg.volume,
        stg.tick_count,

        -- Moving Averages (from Gold layer via staging)
        stg.ma_7,
        stg.ma_20,
        stg.ma_50,

        -- Derived Metrics (computed here in Mart)
        stg.candle_range,
        stg.typical_price,

        -- VWAP cumulative intraday
        vwap_calc.vwap_cumulative,

        -- Price change % vs previous candle
        CASE
            WHEN pc.prev_close IS NULL OR pc.prev_close = 0.0
                THEN NULL
            ELSE
                ROUND(
                    (stg.close - pc.prev_close) / pc.prev_close * 100.0,
                    4
                )
        END                               AS price_change_pct,

        -- Candle direction label for Power BI conditional formatting
        CASE
            WHEN stg.close >= stg.open THEN 'BULLISH'
            ELSE                            'BEARISH'
        END                               AS candle_direction

    FROM stg
    LEFT JOIN vwap_calc
        ON  stg.symbol         = vwap_calc.symbol
        AND stg.trade_date     = vwap_calc.trade_date
        AND stg.window_minutes = vwap_calc.window_minutes
        AND stg.candle_time    = vwap_calc.candle_time
    LEFT JOIN price_change pc
        ON  stg.symbol         = pc.symbol
        AND stg.window_minutes = pc.window_minutes
        AND stg.candle_time    = pc.candle_time

    -- Limit to last 30 days for Power BI performance
    WHERE stg.trade_date >= CURRENT_DATE - INTERVAL '30' DAY
)

SELECT * FROM final
ORDER BY symbol, window_minutes, candle_time DESC
