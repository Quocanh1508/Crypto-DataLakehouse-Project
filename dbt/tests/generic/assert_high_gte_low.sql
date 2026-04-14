-- =============================================================================
-- tests/generic/assert_high_gte_low.sql
-- =============================================================================
-- TYPE: Generic Test (reusable, parameterized)
--
-- PURPOSE:
--   Verify OHLC invariant: High MUST always >= Low in any candle.
--   If high < low, it is a CRITICAL BUG in Gold layer aggregation.
--
-- USAGE in schema.yml:
--   - name: high
--     tests:
--       - assert_high_gte_low:
--           column_high: high
--           column_low: low
--
-- CONVENTION: PASS = 0 rows, FAIL = > 0 rows (violations found)
-- =============================================================================

{% test assert_high_gte_low(model, column_name, column_high, column_low) %}
    SELECT
        *
    FROM {{ model }}
    WHERE
        {{ column_high }} < {{ column_low }}

{% endtest %}
