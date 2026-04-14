-- =============================================================================
-- tests/singular/test_no_null_symbol.sql
-- =============================================================================
-- TYPE: Singular Test
--
-- PURPOSE:
--   Verify Gold table has NO rows with NULL or empty symbol.
--   symbol is the natural primary key - if NULL, data cannot be attributed
--   to any coin and is useless for BI.
--
-- CONVENTION:
--   PASS = 0 rows returned (no violations)
--   FAIL = > 0 rows returned (violations found)
-- =============================================================================

SELECT *
FROM {{ source('gold', 'gold_ohlcv') }}
WHERE
    symbol IS NULL
    OR TRIM(symbol) = ''
