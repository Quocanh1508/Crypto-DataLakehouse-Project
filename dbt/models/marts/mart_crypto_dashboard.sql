-- =============================================================================
-- mart_crypto_dashboard.sql
-- =============================================================================
-- TẦNG: Marts (materialized as TABLE)
-- NGUỒN: {{ ref('stg_gold_ohlcv') }} = staging view vừa tạo ở trên
-- ĐÍCH: Power BI Desktop kết nối trực tiếp bảng này qua Trino ODBC
--
-- MỤC ĐÍCH của Mart layer:
--   1. Tính thêm các chỉ số phân tích nâng cao: VWAP, price_change_pct
--   2. Thêm nhãn candle_direction (BULLISH/BEARISH) cho conditional formatting
--   3. Ghi kết quả thành TABLE thật trên Trino -> Power BI query nhanh
--   4. Giới hạn dữ liệu 30 ngày gần nhất để tránh full scan partition
--
-- TẠI SAO DÙNG TABLE thay vì VIEW?
--   - Power BI cần query lặp đi lặp lại nhiều lần khi user interact với dashboard
--   - Nếu là view, mỗi click filter = Trino tính lại toàn bộ từ Gold files
--   - Table = kết quả đã được compute và lưu sẵn, query O(n) thay vì O(n*k)
--
-- CÁC CHỈ SỐ KỸ THUẬT ĐƯỢC TÍNH:
--
--   VWAP (Volume Weighted Average Price):
--     = Sum(typical_price * volume) / Sum(volume)
--     Tính lũy kế trong ngày (cumulative intraday VWAP)
--     Ý nghĩa: "fair price" của coin trong ngày, traders dùng để
--     xác định entry/exit point. Giá trên VWAP = bullish zone.
--
--   price_change_pct:
--     = (close - prev_close) / prev_close * 100%
--     Tính bằng LAG window function.
--     Dùng trong Power BI để hiện màu xanh (tăng) / đỏ (giảm).
--
--   candle_direction:
--     'BULLISH' nếu close >= open (nến xanh/tăng)
--     'BEARISH' nếu close < open  (nến đỏ/giảm)
-- =============================================================================

WITH stg AS (
    /*
     * {{ ref('stg_gold_ohlcv') }} là cú pháp dbt để tham chiếu model khác.
     * dbt tự resolve thành tên view/table thật trong Trino.
     * Lợi ích: dbt build dependency graph từ các ref() -> biết thứ tự chạy.
     * Nếu stg_gold_ohlcv chưa tồn tại, dbt sẽ báo lỗi dependency trước khi chạy.
     */
    SELECT * FROM {{ ref('stg_gold_ohlcv') }}
),

-- ==========================================================================
-- CTE 1: Tính VWAP lũy kế trong ngày
-- ==========================================================================
-- VWAP = Volume Weighted Average Price
-- Công thức tích lũy (cumulative) theo từng nến trong ngày:
--   numerator   = Sum(typical_price * volume) từ nến đầu ngày đến nến hiện tại
--   denominator = Sum(volume) từ nến đầu ngày đến nến hiện tại
--
-- Tại sao PARTITION BY (symbol, trade_date, window_minutes)?
--   - Mỗi symbol tính VWAP riêng (BTC khác ETH)
--   - Reset về 0 mỗi ngày (intraday VWAP, không cộng dồn qua ngày)
--   - Tính riêng cho nến 1-phút và 5-phút
--
-- NULLIF(..., 0): phòng trường hợp tổng volume = 0 (chia 0 gây lỗi / Infinity)
vwap_calc AS (
    SELECT
        symbol,
        trade_date,
        window_minutes,
        window_start,
        SUM(typical_price * volume) OVER (
            PARTITION BY symbol, trade_date, window_minutes
            ORDER BY window_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        /
        NULLIF(
            SUM(volume) OVER (
                PARTITION BY symbol, trade_date, window_minutes
                ORDER BY window_start
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        )                                 AS vwap_cumulative
    FROM stg
),

-- ==========================================================================
-- CTE 2: Tính % thay đổi giá so với nến ngay trước
-- ==========================================================================
-- LAG(close, 1): lấy giá close của nến liền trước (offset = 1)
-- PARTITION BY (symbol, window_minutes): tính riêng cho từng coin và timeframe
-- ORDER BY window_start: đảm bảo thứ tự thời gian đúng
--
-- Kết quả NULL ở nến đầu tiên của mỗi symbol (không có "nến trước")
-- -> Đây là hành vi đúng, không phải lỗi dữ liệu.
price_change AS (
    SELECT
        symbol,
        window_minutes,
        window_start,
        close,
        LAG(close, 1) OVER (
            PARTITION BY symbol, window_minutes
            ORDER BY window_start
        )                                 AS prev_close
    FROM stg
),

-- ==========================================================================
-- CTE 3: Final assembly — join tất cả CTEs lại
-- ==========================================================================
final AS (
    SELECT
        -- ── Identifiers ─────────────────────────────────────────────────────
        stg.symbol,
        stg.window_start,
        stg.window_end,
        stg.window_minutes,
        stg.trade_date,

        -- ── OHLCV ───────────────────────────────────────────────────────────
        stg.open,
        stg.high,
        stg.low,
        stg.close,
        stg.volume,
        stg.trade_count,

        -- ── Moving Averages (từ Gold layer) ─────────────────────────────────
        stg.sma_5,
        stg.sma_20,

        -- ── Derived Metrics (tính tại Mart) ─────────────────────────────────
        stg.candle_range,
        stg.typical_price,

        -- VWAP lũy kế trong ngày (NULL cho nến đầu ngày vì chưa đủ volume)
        vwap_calc.vwap_cumulative,

        -- % thay đổi giá so với nến trước
        -- CASE: bảo vệ khỏi division by zero nếu prev_close = 0
        -- ROUND 4 chữ số thập phân: đủ cho hiển thị dashboard (0.0123%)
        CASE
            WHEN pc.prev_close IS NULL OR pc.prev_close = 0.0
                THEN NULL
            ELSE
                ROUND(
                    (stg.close - pc.prev_close) / pc.prev_close * 100.0,
                    4
                )
        END                               AS price_change_pct,

        -- Nhãn xu hướng nến: dùng cho conditional formatting màu trong Power BI
        -- BULLISH = nến xanh (close >= open: giá đi lên hoặc flat)
        -- BEARISH = nến đỏ  (close < open:  giá đi xuống)
        CASE
            WHEN stg.close >= stg.open THEN 'BULLISH'
            ELSE                            'BEARISH'
        END                               AS candle_direction

    FROM stg
    -- LEFT JOIN để không mất row nào nếu VWAP hoặc price_change không join được
    LEFT JOIN vwap_calc
        ON  stg.symbol         = vwap_calc.symbol
        AND stg.trade_date     = vwap_calc.trade_date
        AND stg.window_minutes = vwap_calc.window_minutes
        AND stg.window_start   = vwap_calc.window_start
    LEFT JOIN price_change pc
        ON  stg.symbol         = pc.symbol
        AND stg.window_minutes = pc.window_minutes
        AND stg.window_start   = pc.window_start

    -- Giới hạn 30 ngày gần nhất để:
    --   1. Tránh full scan toàn bộ Gold table (có thể vài tháng dữ liệu)
    --   2. Giữ mart nhỏ gọn, Power BI query nhanh
    --   3. Lịch sử đầy đủ vẫn còn trong Gold table gốc nếu cần ad-hoc query
    WHERE stg.trade_date >= CURRENT_DATE - INTERVAL '30' DAY
)

SELECT * FROM final
-- Sắp xếp để dữ liệu mới nhất lên đầu (Power BI mặc định hiển thị top rows)
ORDER BY symbol, window_minutes, window_start DESC
