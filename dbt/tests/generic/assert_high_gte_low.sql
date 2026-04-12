-- =============================================================================
-- tests/generic/assert_high_gte_low.sql
-- =============================================================================
-- LOẠI: Generic Test (tái sử dụng được, có tham số)
--
-- MỤC ĐÍCH:
--   Kiểm tra ràng buộc logic cơ bản nhất của dữ liệu OHLC:
--   High (giá cao nhất) PHẢI luôn >= Low (giá thấp nhất) trong một nến.
--
--   Nếu high < low là BUG NGHIÊM TRỌNG ở Gold layer (silver_to_gold.py
--   aggregate sai MIN/MAX, hoặc dữ liệu Silver bị corrupt).
--
-- CÚ PHÁP Jinja (dbt generic test):
--   {% test test_name(model, param1, param2) %}
--     ... SQL trả về rows VI PHẠM điều kiện ...
--   {% endtest %}
--
--   dbt convention: test PASS = 0 rows, test FAIL = > 0 rows
--   -> Query này SELECT những rows lỗi, nếu không có rows = test pass.
--
-- CÁCH DÙNG trong schema.yml:
--   - name: high
--     tests:
--       - assert_high_gte_low:
--           column_high: high
--           column_low: low
--
-- TẠI SAO VIẾT GENERIC thay vì SINGULAR TEST?
--   - Generic test có thể gắn vào bất kỳ model nào (staging, mart, source)
--   - Tham số hoá: dùng được cho bất kỳ cặp cột high/low nào
--   - Viết 1 lần, test N models
-- =============================================================================

{% test assert_high_gte_low(model, column_high, column_low) %}
    /*
     * {{ model }}        : dbt tự điền tên bảng/view đang được test
     * {{ column_high }}  : tên cột "high" (truyền từ schema.yml)
     * {{ column_low }}   : tên cột "low"  (truyền từ schema.yml)
     *
     * Query này trả về TẤT CẢ rows vi phạm ràng buộc high >= low.
     * dbt chạy query này, nếu count > 0 -> test FAIL + in ra rows lỗi.
     */
    SELECT
        *
    FROM {{ model }}
    WHERE
        -- Vi phạm: high < low là dữ liệu bất khả thi trong thực tế
        {{ column_high }} < {{ column_low }}

{% endtest %}
