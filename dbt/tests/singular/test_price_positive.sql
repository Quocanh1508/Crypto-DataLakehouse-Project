-- =============================================================================
-- tests/singular/test_price_positive.sql
-- =============================================================================
-- TYPE: Singular Test
--
-- PURPOSE:
--   Verify all OHLC prices are positive (> 0) and volume is non-negative (>= 0).
--   No crypto asset can have negative or zero price on Binance.
--   Negative volume is economically meaningless and indicates a bug.
--
-- RULES:
--   open/high/low/close: MUST > 0 (Binance never has price = 0)
--   volume: MUST >= 0 (candle may have 0 trades, but never negative)
-- =============================================================================

SELECT *
FROM {{ source('gold', 'gold_ohlcv') }}
WHERE
    open  <= 0 OR open  IS NULL
    OR high  <= 0 OR high  IS NULL
    OR low   <= 0 OR low   IS NULL
    OR close <= 0 OR close IS NULL
    OR volume < 0 OR volume IS NULL
