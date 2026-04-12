-- =============================================================================
-- tests/singular/test_ohlcv_unique_candle.sql
-- =============================================================================
-- LOẠI: Singular Test
--
-- MỤC ĐÍCH:
--   Mỗi nến phải là DUY NHẤT trong Gold table.
--   Khóa tự nhiên: (symbol, window_start, window_minutes) phải unique.
--
-- TẠI SAO CÓ THỂ BỊ DUPLICATE?
--   1. silver_to_gold.py chạy 2 lần không có idempotency check
--   2. Airflow trigger job 2 lần với cùng 1 partition date
--   3. Delta Lake merge bị lỗi -> ghi 2 lần
--   4. Spark chạy lại từ checkpoint cũ
--
-- CÁCH PHÁT HIỆN DUPLICATE:
--   GROUP BY primary key, đếm số lần xuất hiện.
--   Nếu count > 1 -> duplicate.
--
-- NẾU TEST NÀY FAIL:
--   1. Chạy OPTIMIZE + VACUUM trên Gold table (Airflow DAG của Teammate 1)
--   2. Xem silver_to_gold.py có MERGE INTO hay chỉ INSERT?
--   3. MERGE INTO theo (symbol, window_start, window_minutes) là đúng
-- =============================================================================

SELECT
    symbol,
    window_start,
    window_minutes,
    COUNT(*) AS duplicate_count
FROM {{ source('gold', 'gold_ohlcv') }}
GROUP BY
    symbol,
    window_start,
    window_minutes
HAVING
    -- Chỉ giữ lại những combination xuất hiện > 1 lần (duplicate)
    COUNT(*) > 1
