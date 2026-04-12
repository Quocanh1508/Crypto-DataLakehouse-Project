-- =============================================================================
-- tests/singular/test_no_null_symbol.sql
-- =============================================================================
-- LOẠI: Singular Test (một file SQL = một test cụ thể)
--
-- MỤC ĐÍCH:
--   Kiểm tra rằng bảng Gold KHÔNG có row nào với symbol là NULL hoặc chuỗi rỗng.
--
-- TẠI SAO QUAN TRỌNG?
--   - symbol là "primary key tự nhiên" — biết coin này là gì
--   - NULL symbol = không thể biết đây là BTC, ETH, hay coin nào
--   - Row NULL symbol KHÔNG thể phục vụ BI (không thể filter, group by)
--   - Nếu test này fail, lỗi có thể ở: producer_stream.py (field 's' bị mất),
--     hoặc bronze_to_silver.py xử lý sai, hoặc silver_to_gold.py bị bug join
--
-- CONVENTION dbt Singular Test:
--   - File đặt trong thư mục tests/singular/
--   - Query trả về rows VI PHẠM (lỗi)
--   - test PASS = 0 rows returned
--   - test FAIL = > 0 rows returned
--
-- TẠI SAO VIẾT SINGULAR thay vì GENERIC TEST?
--   - Test này chỉ áp dụng cho source 'gold_ohlcv', không generic
--   - Logic đặc thù: check cả NULL lẫn empty string cùng 1 lúc
--   - Dễ đọc và maintain hơn khi logic phức tạp
-- =============================================================================

SELECT *
FROM {{ source('gold', 'gold_ohlcv') }}
WHERE
    -- Trường hợp 1: symbol là NULL hoàn toàn
    symbol IS NULL

    -- Trường hợp 2: symbol là chuỗi rỗng hoặc chỉ có whitespace
    -- TRIM loại bỏ spaces ở đầu/cuối trước khi so sánh
    OR TRIM(symbol) = ''
