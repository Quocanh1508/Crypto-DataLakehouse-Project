-- =============================================================================
-- scripts/create_mock_gold_trino.sql
-- =============================================================================
-- Mục đích: Tạo bảng gold_ohlcv trong Trino (Delta Lake connector)
-- và nạp dữ liệu mẫu vào để test dbt khi Teammate 1 chưa xong.
--
-- CÁCH CHẠY:
--   Mở trình duyệt: http://localhost:8080  (Trino UI)
--   Click "Query Editor" -> copy từng block SQL bên dưới -> Run
--
-- THỨ TỰ CHẠY: Bước 1 -> Bước 2 -> Bước 3 -> Bước 4
-- =============================================================================


-- ═══════════════════════════════════════════════════════════════════════════
-- BƯỚC 1: Kiểm tra Trino catalog và schema đang có
-- Mục đích: Đảm bảo catalog 'delta' đã được load, schema 'default' tồn tại
-- ═══════════════════════════════════════════════════════════════════════════
SHOW CATALOGS;
-- Expected: phải có 'delta' trong danh sách

SHOW SCHEMAS FROM delta;
-- Expected: phải có 'default' trong danh sách


-- ═══════════════════════════════════════════════════════════════════════════
-- BƯỚC 2: Xoá bảng cũ nếu có (để tạo lại sạch)
-- Mục đích: Tránh lỗi "Table already exists" khi chạy lại script
-- ═══════════════════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS delta.default.gold_ohlcv;


-- ═══════════════════════════════════════════════════════════════════════════
-- BƯỚC 3: Tạo bảng gold_ohlcv trong Delta Lake trên MinIO
--
-- Lý do dùng WITH (location = 's3://gold/ohlcv/'):
--   - Trino Delta Lake connector đọc/ghi Delta format vào MinIO bucket 'gold'
--   - Bucket 'gold' đã được tạo sẵn bởi service 'mc' trong docker-compose.yml
--   - Trino dùng hive.s3.* settings trong delta.properties để connect MinIO
--
-- Schema này PHẢI khớp với cột mà silver_to_gold.py của Teammate 1 sẽ tạo.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE delta.default.gold_ohlcv (
    symbol          VARCHAR,            -- Tên cặp coin, ví dụ BTCUSDT
    window_start    TIMESTAMP(6),       -- Timestamp bắt đầu nến (UTC, microseconds)
    window_end      TIMESTAMP(6),       -- Timestamp kết thúc nến
    window_minutes  INTEGER,            -- Độ dài nến: 1 hoặc 5
    open            DOUBLE,             -- Giá mở cửa
    high            DOUBLE,             -- Giá cao nhất
    low             DOUBLE,             -- Giá thấp nhất
    close           DOUBLE,             -- Giá đóng cửa
    volume          DOUBLE,             -- Khối lượng giao dịch
    trade_count     BIGINT,             -- Số tick giao dịch trong nến
    sma_5           DOUBLE,             -- Simple Moving Average 5 kỳ
    sma_20          DOUBLE              -- Simple Moving Average 20 kỳ
)
WITH (
    -- Trỏ đến bucket 'gold' trong MinIO (đã tạo sẵn bởi docker-compose mc service)
    location = 's3://gold/ohlcv/'
);

-- Kiểm tra bảng đã được tạo
DESCRIBE delta.default.gold_ohlcv;


-- ═══════════════════════════════════════════════════════════════════════════
-- BƯỚC 4: Nạp dữ liệu mẫu vào bảng
--
-- Dữ liệu bao gồm:
--   - BTCUSDT: 5 nến 1-phút + 1 nến 5-phút  (coin lớn, giá cao)
--   - ETHUSDT: 5 nến 1-phút + 1 nến 5-phút  (coin lớn, giá trung)
--   - BNBUSDT: 5 nến 1-phút + 1 nến 5-phút  (coin lớn, giá thấp hơn)
--   - PEPEUSDT: 2 nến 1-phút                 (meme coin, giá rất nhỏ)
--   - SHIBUSDT: 2 nến 1-phút                 (meme coin, giá rất nhỏ)
--
-- PEPE/SHIB được thêm đặc biệt để test Decimal precision (giá 0.0000xxx)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO delta.default.gold_ohlcv VALUES
-- ── BTCUSDT: 5 nến 1-phút ─────────────────────────────────────────────────
('BTCUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1, 69420.50, 69500.00, 69380.00, 69455.00, 12.5, 350, 69430.00, 69200.00),
('BTCUSDT', TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1, 69455.00, 69520.00, 69400.00, 69510.00, 15.2, 420, 69450.00, 69210.00),
('BTCUSDT', TIMESTAMP '2026-04-12 10:02:00', TIMESTAMP '2026-04-12 10:03:00', 1, 69510.00, 69580.00, 69490.00, 69530.00, 11.8, 390, 69480.00, 69215.00),
('BTCUSDT', TIMESTAMP '2026-04-12 10:03:00', TIMESTAMP '2026-04-12 10:04:00', 1, 69530.00, 69550.00, 69460.00, 69490.00,  9.3, 310, 69495.00, 69218.00),
('BTCUSDT', TIMESTAMP '2026-04-12 10:04:00', TIMESTAMP '2026-04-12 10:05:00', 1, 69490.00, 69510.00, 69400.00, 69420.00, 14.1, 450, 69481.00, 69222.00),
-- ── BTCUSDT: 1 nến 5-phút ─────────────────────────────────────────────────
('BTCUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:05:00', 5, 69420.50, 69580.00, 69380.00, 69420.00, 63.0, 1920, 69450.00, 69200.00),

-- ── ETHUSDT: 5 nến 1-phút ─────────────────────────────────────────────────
('ETHUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1, 3500.00, 3510.00, 3490.00, 3505.00, 85.3, 210, 3500.00, 3480.00),
('ETHUSDT', TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1, 3505.00, 3520.00, 3500.00, 3515.00, 92.1, 235, 3505.00, 3482.00),
('ETHUSDT', TIMESTAMP '2026-04-12 10:02:00', TIMESTAMP '2026-04-12 10:03:00', 1, 3515.00, 3530.00, 3510.00, 3525.00, 78.4, 198, 3510.00, 3484.00),
('ETHUSDT', TIMESTAMP '2026-04-12 10:03:00', TIMESTAMP '2026-04-12 10:04:00', 1, 3525.00, 3535.00, 3515.00, 3520.00, 65.9, 175, 3515.00, 3486.00),
('ETHUSDT', TIMESTAMP '2026-04-12 10:04:00', TIMESTAMP '2026-04-12 10:05:00', 1, 3520.00, 3525.00, 3505.00, 3510.00, 88.7, 220, 3515.00, 3488.00),
-- ── ETHUSDT: 1 nến 5-phút ─────────────────────────────────────────────────
('ETHUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:05:00', 5, 3500.00, 3535.00, 3490.00, 3510.00, 410.4, 1038, 3505.00, 3480.00),

-- ── BNBUSDT: 5 nến 1-phút ─────────────────────────────────────────────────
('BNBUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1, 580.00, 582.00, 579.00, 581.50, 50.0, 180, 580.00, 575.00),
('BNBUSDT', TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1, 581.50, 583.00, 580.50, 582.00, 45.2, 165, 580.50, 575.50),
('BNBUSDT', TIMESTAMP '2026-04-12 10:02:00', TIMESTAMP '2026-04-12 10:03:00', 1, 582.00, 584.00, 581.00, 583.50, 52.8, 192, 581.50, 576.00),
('BNBUSDT', TIMESTAMP '2026-04-12 10:03:00', TIMESTAMP '2026-04-12 10:04:00', 1, 583.50, 585.00, 582.50, 584.00, 48.1, 170, 582.30, 576.50),
('BNBUSDT', TIMESTAMP '2026-04-12 10:04:00', TIMESTAMP '2026-04-12 10:05:00', 1, 584.00, 584.50, 582.00, 583.00, 41.7, 155, 582.80, 577.00),
-- ── BNBUSDT: 1 nến 5-phút ─────────────────────────────────────────────────
('BNBUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:05:00', 5, 580.00, 585.00, 579.00, 583.00, 237.8, 862, 581.00, 575.00),

-- ── PEPEUSDT & SHIBUSDT: meme coins giá rất nhỏ (test decimal precision) ──
('PEPEUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1, 0.00001234, 0.00001256, 0.00001220, 0.00001245, 5000000.0, 450, 0.00001240, 0.00001210),
('PEPEUSDT', TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1, 0.00001245, 0.00001270, 0.00001238, 0.00001260, 4800000.0, 425, 0.00001248, 0.00001214),
('SHIBUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1, 0.00002345, 0.00002380, 0.00002330, 0.00002360, 3000000.0, 320, 0.00002350, 0.00002300),
('SHIBUSDT', TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1, 0.00002360, 0.00002400, 0.00002355, 0.00002390, 2900000.0, 305, 0.00002365, 0.00002305);


-- ═══════════════════════════════════════════════════════════════════════════
-- BƯỚC 5: Verify dữ liệu đã insert thành công
-- ═══════════════════════════════════════════════════════════════════════════
-- Kiểm tra tổng số rows (expected: 24 rows)
SELECT COUNT(*) AS total_rows FROM delta.default.gold_ohlcv;

-- Kiểm tra phân bố theo symbol và window_minutes
SELECT
    symbol,
    window_minutes,
    COUNT(*) AS candle_count,
    MIN(low)  AS min_price,
    MAX(high) AS max_price
FROM delta.default.gold_ohlcv
GROUP BY symbol, window_minutes
ORDER BY symbol, window_minutes;

-- Preview 5 rows đầu tiên
SELECT * FROM delta.default.gold_ohlcv LIMIT 5;
