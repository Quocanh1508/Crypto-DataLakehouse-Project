-- =============================================================================
-- tests/singular/test_no_missing_1min_candles.sql
-- =============================================================================
-- TYPE: Singular Test (advanced - time series completeness check)
--
-- PURPOSE:
--   Detect gaps in the 1-minute candle time series.
--   Binance trades are highly active, so 1-min candles should be continuous.
--
-- SCOPE: Only checks the last 2 hours to avoid full table scan.
--
-- LOGIC:
--   1. Get active symbols (have data in last 2 hours)
--   2. Generate expected time series: 1 minute per row, 2 hours
--   3. LEFT JOIN with actual data -> NULL right side = missing minute
--
-- COLUMN CHANGES:
--   Old: window_start, window_minutes = 1
--   New: candle_time, candle_duration = '1 minute'
--
-- NOTE: This test will FAIL on mock/historical data. It is designed
-- for live/recent data only. Exclude via:
--   dbt test --exclude test_no_missing_1min_candles
-- =============================================================================

WITH
-- Step 1: Get active symbols with 1-min candles in the last 2 hours
active_symbols AS (
    SELECT DISTINCT symbol
    FROM {{ source('gold', 'gold_ohlcv') }}
    WHERE
        candle_duration = '1 minute'
        AND candle_time >= (NOW() - INTERVAL '2' HOUR)
),

-- Step 2: Generate expected time series (1 row per minute, 2 hours)
expected_time_series AS (
    SELECT
        s.symbol,
        ts_table.expected_minute
    FROM active_symbols s
    CROSS JOIN (
        SELECT ts AS expected_minute
        FROM UNNEST(
            SEQUENCE(
                DATE_TRUNC('minute', NOW() - INTERVAL '2' HOUR),
                DATE_TRUNC('minute', NOW()),
                INTERVAL '1' MINUTE
            )
        ) AS t(ts)
    ) AS ts_table
),

-- Step 3: Actual candles from Gold table
actual_candles AS (
    SELECT
        symbol,
        DATE_TRUNC('minute', candle_time) AS actual_minute
    FROM {{ source('gold', 'gold_ohlcv') }}
    WHERE
        candle_duration = '1 minute'
        AND candle_time >= (NOW() - INTERVAL '2' HOUR)
)

-- Step 4: Find missing minutes via LEFT JOIN
SELECT
    e.symbol,
    e.expected_minute  AS missing_minute,
    'MISSING 1-min candle detected' AS test_message
FROM expected_time_series e
LEFT JOIN actual_candles a
    ON e.symbol = a.symbol
    AND e.expected_minute = a.actual_minute
WHERE
    a.actual_minute IS NULL
