-- =============================================================================
-- tests/singular/test_no_missing_1min_candles.sql
-- =============================================================================
-- LOẠI: Singular Test (nâng cao — time series completeness check)
--
-- MỤC ĐÍCH:
--   Kiểm tra không có khoảng trống (gap) trong chuỗi nến 1-phút.
--   Binance trade rất active, nến 1 phút phải liên tục không đứt đoạn.
--
--   Nếu có gap: có thể do:
--     1. WebSocket bị ngắt kết nối (producer_stream.py reconnect chậm)
--     2. Spark micro-batch bị fail (bronze_streaming.py checkpoint corrupt)
--     3. silver_to_gold.py bị crash giữa chừng
--     4. Kafka lag quá lớn (consumer không kịp đọc)
--
-- PHẠM VI KIỂM TRA:
--   Chỉ kiểm tra 2 giờ gần nhất (INTERVAL '2' HOUR) để:
--   - Tránh full scan toàn bộ Gold table (có thể vài tháng data)
--   - Phát hiện vấn đề real-time nhanh nhất
--   - Giảm query cost trên Trino
--
-- LOGIC:
--   1. Lấy danh sách symbols đang active (có data trong 2 giờ qua)
--   2. Generate chuỗi thời gian lý thuyết: mỗi phút 1 mốc, trong 2 giờ
--   3. LEFT JOIN với data thật -> rows NULL ở right side = phút bị thiếu
--
-- LƯU Ý: Test này có thể SLOW nếu chạy với nhiều symbols.
-- Trino SEQUENCE + UNNEST tạo temporary table trong memory.
-- Disable test này nếu cần: thêm config dưới đây vào dbt_project.yml:
--
--   tests:
--     crypto_lakehouse:
--       +enabled: false   (cho test folder cụ thể)
--
-- Hoặc skip khi chạy: dbt test --exclude test_no_missing_1min_candles
-- =============================================================================

WITH
-- Bước 1: Lấy danh sách symbols đang active trong 2 giờ qua
-- Không hardcode danh sách symbols để tự động thích nghi khi Top-50 thay đổi
active_symbols AS (
    SELECT DISTINCT symbol
    FROM {{ source('gold', 'gold_ohlcv') }}
    WHERE
        window_minutes = 1
        AND window_start >= (NOW() - INTERVAL '2' HOUR)
),

-- Bước 2: Tạo chuỗi thời gian lý thuyết (1 mốc mỗi phút trong 2 giờ)
-- Trino SEQUENCE(start, stop, step): tạo array timestamps
-- UNNEST: mở array thành rows
-- CROSS JOIN với active_symbols: mỗi symbol × mỗi phút = 1 row expected
expected_time_series AS (
    SELECT
        s.symbol,
        ts_table.expected_minute
    FROM active_symbols s
    CROSS JOIN (
        SELECT ts AS expected_minute
        FROM UNNEST(
            SEQUENCE(
                DATE_TRUNC('minute', NOW() - INTERVAL '2' HOUR),  -- bắt đầu: 2 giờ trước (làm tròn xuống phút)
                DATE_TRUNC('minute', NOW()),                       -- kết thúc: hiện tại (làm tròn xuống phút)
                INTERVAL '1' MINUTE                               -- bước: 1 phút
            )
        ) AS t(ts)
    ) AS ts_table
),

-- Bước 3: Data thật từ Gold table
-- DATE_TRUNC('minute', window_start) để normalize về phút chẵn
-- (vì window_start có thể có giây/milliseconds)
actual_candles AS (
    SELECT
        symbol,
        DATE_TRUNC('minute', window_start) AS actual_minute
    FROM {{ source('gold', 'gold_ohlcv') }}
    WHERE
        window_minutes = 1
        AND window_start >= (NOW() - INTERVAL '2' HOUR)
)

-- Bước 4: Tìm các phút bị thiếu bằng LEFT JOIN + NULL check
-- LEFT JOIN: giữ toàn bộ expected_time_series, kể cả khi không có actual data
-- WHERE a.actual_minute IS NULL: những expected minute không có actual data = GAP
SELECT
    e.symbol,
    e.expected_minute  AS missing_minute,
    'MISSING 1-min candle detected' AS test_message
FROM expected_time_series e
LEFT JOIN actual_candles a
    ON e.symbol = a.symbol
    AND e.expected_minute = a.actual_minute
WHERE
    a.actual_minute IS NULL    -- phút này không có nến trong Gold table -> GAP
