-- =============================================================================
-- tests/singular/test_price_positive.sql
-- =============================================================================
-- LOẠI: Singular Test
--
-- MỤC ĐÍCH:
--   Kiểm tra tất cả giá (open, high, low, close) phải DƯƠNG (> 0).
--   Volume không được âm (có thể = 0 trong nến rỗng, nhưng không âm).
--
-- TẠI SAO QUAN TRỌNG?
--   - Trong thực tế, không có coin nào có giá âm (crypto không giống futures)
--   - Giá = 0: lỗi khi Silver không filter được bad records trước khi Gold aggregate
--   - Giá âm: lỗi nghiêm trọng trong kiểu dữ liệu hoặc tính toán
--   - Volume âm: kinh tế học vô nghĩa, chắc chắn là bug
--
-- PHÂN BIỆT:
--   - open/high/low/close: PHẢI > 0 (không cho phép = 0 vì Binance không có giá = 0)
--   - volume: PHẢI >= 0 (nến có thể có 0 ticks nếu không có giao dịch nào trong khoảng đó)
--     Tuy nhiên, volume < 0 là BUG.
--
-- NẾU TEST NÀY FAIL:
--   1. Xem quarantine table trong Silver: silver.quarantine không filter hết bad rows?
--   2. Silver's cast_and_enrich() bị lỗi với một số symbol đặc biệt
--   3. silver_to_gold.py aggregate MIN/MAX incorrect (MIN âm từ quarantine)
-- =============================================================================

SELECT *
FROM {{ source('gold', 'gold_ohlcv') }}
WHERE
    -- Giá mở cửa phải dương
    open  <= 0 OR open  IS NULL

    -- Giá cao nhất phải dương (và >= low, được test riêng bởi assert_high_gte_low)
    OR high  <= 0 OR high  IS NULL

    -- Giá thấp nhất phải dương
    OR low   <= 0 OR low   IS NULL

    -- Giá đóng cửa phải dương
    OR close <= 0 OR close IS NULL

    -- Volume không được âm (= 0 là chấp nhận được cho nến không có giao dịch)
    OR volume < 0 OR volume IS NULL
