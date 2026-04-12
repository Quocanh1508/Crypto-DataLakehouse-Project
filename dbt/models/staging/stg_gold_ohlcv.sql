-- =============================================================================
-- stg_gold_ohlcv.sql
-- =============================================================================
-- TẦNG: Staging (materialized as VIEW)
-- NGUỒN: {{ source('gold', 'gold_ohlcv') }} = delta.default.gold_ohlcv trên Trino
-- ĐÍCHTừ: mart_crypto_dashboard.sql sẽ SELECT FROM model này
--
-- MỤC ĐÍCH của Staging layer:
--   1. Chuẩn hoá kiểu dữ liệu (cast từ Decimal/String sang Double)
--   2. Thêm cột tính toán phụ trợ (candle_range, typical_price, window_end)
--   3. Lọc bỏ dữ liệu bất thường cơ bản (window_start trong tương lai)
--   4. Tạo cột trade_date (DATE) phục vụ partition pruning trong Power BI
--
-- TẠI SAO DÙNG VIEW thay vì TABLE?
--   - Staging chỉ là bước làm sạch trung gian, không cần lưu vào storage
--   - View = Trino tự compute khi có request, không tốn GCS space
--   - Grạy dbt run nhanh hơn (không cần ghi Parquet files)
--
-- LƯU Ý KỸ THUẬT:
--   - Trino Delta Lake đọc giá trị Decimal từ Gold table
--   - CAST AS DOUBLE để tránh lỗi arithmetic với DecimalType trong window functions
--   - INTERVAL '1' MINUTE * window_minutes: Trino hỗ trợ nhân interval với số nguyên
-- =============================================================================

WITH source AS (
    /*
     * {{ source('gold', 'gold_ohlcv') }} là cú pháp Jinja template của dbt.
     * dbt tự resolve thành: delta.default.gold_ohlcv
     * Lợi ích: nếu bảng Gold đổi schema, chỉ sửa sources.yml, không cần sửa file này.
     */
    SELECT * FROM {{ source('gold', 'gold_ohlcv') }}
),

enriched AS (
    SELECT
        -- ── Khóa định danh (Primary Key tự nhiên của nến) ──────────────────
        symbol,
        window_start,
        window_minutes,

        -- ── OHLCV Core ─────────────────────────────────────────────────────
        -- Cast về DOUBLE để Trino tính window functions (VWAP, SMA) chính xác.
        -- Gold layer lưu dưới dạng Decimal(38,18) từ Silver, DOUBLE đủ precision
        -- cho display và BI purposes (không cần 18 chữ số thập phân ở dashboard).
        CAST(open   AS DOUBLE) AS open,
        CAST(high   AS DOUBLE) AS high,
        CAST(low    AS DOUBLE) AS low,
        CAST(close  AS DOUBLE) AS close,
        CAST(volume AS DOUBLE) AS volume,

        -- ── Moving Averages từ Gold layer ──────────────────────────────────
        -- Giữ nguyên từ Gold, không tính lại ở đây (Gold đã làm)
        sma_5,
        sma_20,

        -- ── Trade Metadata ─────────────────────────────────────────────────
        trade_count,

        -- ── Cột phụ trợ: Candle Range ──────────────────────────────────────
        -- Ý nghĩa: biên độ dao động giá trong 1 nến (high - low)
        -- Dùng trong Power BI để visualize volatility.
        -- Nến range rộng = thị trường biến động mạnh.
        CAST(high AS DOUBLE) - CAST(low AS DOUBLE)      AS candle_range,

        -- ── Cột phụ trợ: Typical Price ─────────────────────────────────────
        -- Công thức: (high + low + close) / 3
        -- Được dùng để tính VWAP ở mart layer.
        -- Lý do không dùng just close: typical price capture cả high/low extremes.
        (CAST(high AS DOUBLE) + CAST(low AS DOUBLE) + CAST(close AS DOUBLE)) / 3.0
                                                         AS typical_price,

        -- ── Cột phụ trợ: Window End Time ──────────────────────────────────
        -- Tính thời điểm kết thúc nến từ window_start.
        -- Trino: INTERVAL '1' MINUTE * integer là hợp lệ.
        -- Dùng trong Power BI timeline axis để hiển thị đúng khoảng thời gian.
        window_start + (INTERVAL '1' MINUTE) * window_minutes
                                                         AS window_end,

        -- ── Cột phân vùng: Trade Date ─────────────────────────────────────
        -- Trích xuất DATE từ timestamp để Power BI filter theo ngày.
        -- Quan trọng: Gold table partition by (symbol, dt), nên WHERE trade_date = X
        -- trong Power BI sẽ kích hoạt Trino partition pruning -> query nhanh hơn nhiều.
        DATE(window_start)                               AS trade_date

    FROM source
    WHERE
        -- Lọc dữ liệu bất thường: window_start không được trong tương lai.
        -- Clock skew tối đa chấp nhận được là 10 phút (để tránh false filter
        -- do timezone difference giữa Binance server và GCS).
        window_start <= (NOW() + INTERVAL '10' MINUTE)
)

SELECT * FROM enriched
