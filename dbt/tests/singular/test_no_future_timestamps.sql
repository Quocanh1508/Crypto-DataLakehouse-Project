-- =============================================================================
-- tests/singular/test_no_future_timestamps.sql
-- =============================================================================
-- TYPE: Singular Test
--
-- PURPOSE:
--   Verify no candle has candle_time in the FUTURE.
--   Future timestamps indicate: clock skew, silver_to_gold.py bug,
--   or corrupted data injected into Kafka.
--
-- WHY 10-MINUTE TOLERANCE?
--   Timezone differences between services may cause a few minutes of drift.
--   10 minutes is a safe buffer: still detects real bugs, avoids false alarms.
--
-- COLUMN CHANGE:
--   Old Gold schema: window_start
--   New Gold schema: candle_time (from silver_to_gold.py)
-- =============================================================================

SELECT *
FROM {{ source('gold', 'gold_ohlcv') }}
WHERE
    candle_time > (NOW() + INTERVAL '10' MINUTE)
