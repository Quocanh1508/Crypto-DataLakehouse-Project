-- =============================================================================
-- tests/singular/test_no_future_timestamps.sql
-- =============================================================================
-- LOẠI: Singular Test
--
-- MỤC ĐÍCH:
--   Kiểm tra không có nến nào có window_start trong TƯƠNG LAI.
--
-- TẠI SAO TƯƠNG LAI LÀ VẤN ĐỀ?
--   1. Clock Skew: đồng hồ server Binance vs server của chúng ta lệch nhau
--   2. Bug trong silver_to_gold.py: tính toán sai window grouping
--   3. Data được inject giả mạo / corrupt vào Kafka
--
-- TẠI SAO CHO PHÉP SAI LỆCH 10 PHÚT?
--   - Timezone khác nhau giữa các services có thể gây lệch vài phút
--   - Binance server ở timezone UTC, local Spark có thể tính sai nếu config sai
--   - 10 phút là buffer an toàn: vẫn detect được lỗi thật nhưng tránh false alarm
--   - Nếu sai lệch > 10 phút: chắc chắn là bug, không phải timezone issue
--
-- NẾU TEST NÀY FAIL:
--   1. Kiểm tra timezone config trong docker-compose.yml (TZ environment variable)
--   2. Kiểm tra silver_to_gold.py cách tính window_start
--   3. Xem xét Binance API response time có bị lag không
-- =============================================================================

SELECT *
FROM {{ source('gold', 'gold_ohlcv') }}
WHERE
    -- window_start không được vượt quá NOW() + 10 phút
    -- (NOW() + INTERVAL '10' MINUTE) = buffer cho clock skew
    window_start > (NOW() + INTERVAL '10' MINUTE)
