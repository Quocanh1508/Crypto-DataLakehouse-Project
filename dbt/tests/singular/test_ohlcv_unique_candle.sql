-- =============================================================================
-- tests/singular/test_ohlcv_unique_candle.sql
-- =============================================================================
-- TYPE: Singular Test
--
-- PURPOSE:
--   Each candle must be UNIQUE in the Gold table.
--   Natural key: (symbol, candle_time, candle_duration) must be unique.
--
-- WHY DUPLICATES MAY EXIST:
--   1. silver_to_gold.py ran twice without idempotency check
--   2. Airflow triggered the job twice for the same partition
--   3. Delta Lake merge failed -> double write
--   4. Spark restarted from stale checkpoint
--
-- COLUMN CHANGES:
--   Old: (symbol, window_start, window_minutes)
--   New: (symbol, candle_time, candle_duration)
-- =============================================================================

SELECT
    symbol,
    candle_time,
    candle_duration,
    COUNT(*) AS duplicate_count
FROM {{ source('gold', 'gold_ohlcv') }}
GROUP BY
    symbol,
    candle_time,
    candle_duration
HAVING
    COUNT(*) > 1
