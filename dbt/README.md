# =============================================================================
# dbt/README.md
# =============================================================================
# Tài liệu giải thích toàn bộ cấu trúc và cách vận hành dbt module.
# =============================================================================

# 🧱 dbt Module — Crypto Data Lakehouse

**Vai trò:** Teammate 2 (Analytics Engineer) — Phase 6 & 7  
**Stack:** dbt Core 1.8 + dbt-trino adapter  
**Query Engine:** Trino 432 (kết nối qua Docker port 8080)  
**Data Source:** `delta.default.gold_ohlcv` (Gold layer của Teammate 1)

---

## 📁 Cấu Trúc Thư Mục

```
dbt/
├── dbt_project.yml            # Config trung tâm: tên project, paths, materialization
├── packages.yml               # Thư viện ngoài: dbt_utils
├── profiles_template.yml      # Template kết nối Trino (KHÔNG commit file thật)
├── .gitignore                 # Bỏ qua target/, dbt_packages/, profiles.yml
├── seeds/
│   ├── mock_gold_ohlcv.csv    # Dữ liệu mẫu khi Gold chưa có
│   └── schema.yml             # Khai báo kiểu dữ liệu seed
├── models/
│   ├── sources.yml            # Khai báo bảng Gold từ Trino
│   ├── staging/
│   │   ├── stg_gold_ohlcv.sql # View: làm sạch, thêm cột phụ trợ
│   │   └── schema.yml         # Tests cho staging model
│   └── marts/
│       ├── mart_crypto_dashboard.sql  # Table: VWAP + price_change + direction
│       └── schema.yml                # Tests cho mart model
└── tests/
    ├── generic/
    │   └── assert_high_gte_low.sql   # Custom test: high >= low
    └── singular/
        ├── test_no_null_symbol.sql           # source test: không NULL symbol
        ├── test_no_future_timestamps.sql     # source test: không timestamp tương lai
        ├── test_price_positive.sql           # source test: giá > 0
        ├── test_no_missing_1min_candles.sql  # source test: không gap nến 1 phút
        └── test_ohlcv_unique_candle.sql      # source test: không duplicate nến
```

---

## 🔄 Data Lineage

```
[delta.default.gold_ohlcv]  ← Teammate 1 tạo (silver_to_gold.py)
           |
           | {{ source('gold', 'gold_ohlcv') }}
           ▼
[stg_gold_ohlcv]            ← VIEW trên Trino
  - Cast Decimal → Double
  - Thêm: candle_range, typical_price, window_end, trade_date
  - Filter: window_start <= NOW() + 10 phút
           |
           | {{ ref('stg_gold_ohlcv') }}
           ▼
[mart_crypto_dashboard]     ← TABLE trên Trino (Power BI đọc từ đây)
  - Tính VWAP lũy kế trong ngày
  - Tính price_change_pct (LAG window)
  - Thêm candle_direction (BULLISH/BEARISH)
  - Filter: 30 ngày gần nhất
```

---

## ⚙️ Setup Lần Đầu

### Bước 1: Cài dbt

```bash
# Tạo venv riêng để tránh conflict với PySpark
python -m venv .venv-dbt
.venv-dbt\Scripts\activate       # Windows
# source .venv-dbt/bin/activate  # Mac/Linux

pip install dbt-core dbt-trino
dbt --version   # kiểm tra cài đặt thành công
```

### Bước 2: Tạo profiles.yml (NGOÀI project folder)

```
Windows: C:\Users\<YourUsername>\.dbt\profiles.yml
```

Nội dung (xem `profiles_template.yml` để copy):

```yaml
crypto_lakehouse:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      host: localhost
      port: 8080
      database: delta
      schema: default
      threads: 4
      http_scheme: http
```

### Bước 3: Đảm bảo Trino đang chạy

```bash
docker-compose up -d trino hive-metastore postgres minio
# Chờ khoảng 30 giây rồi test:
curl http://localhost:8080/v1/info
```

### Bước 4: Kiểm tra kết nối

```bash
cd dbt
dbt debug
# Expected: "All checks passed!"
```

### Bước 5: Cài packages

```bash
dbt deps
# Expected: dbt_utils được downloaded vào dbt_packages/
```

---

## 🚀 Lệnh Hay Dùng

```bash
# ─── Chạy toàn bộ pipeline ─────────────────────────────────────────
dbt run                              # Build staging + mart tables/views
dbt test                             # Chạy tất cả tests
dbt run && dbt test                  # Combo: build rồi test ngay

# ─── Chạy từng model riêng lẻ ──────────────────────────────────────
dbt run --select stg_gold_ohlcv             # Chỉ staging model
dbt run --select mart_crypto_dashboard      # Chỉ mart model
dbt test --select stg_gold_ohlcv            # Test staging only

# ─── Khi Teammate 1 chưa xong — dùng mock data ─────────────────────
dbt seed                             # Load mock_gold_ohlcv.csv lên Trino
dbt seed --full-refresh              # Xoá và load lại (nếu CSV thay đổi)

# ─── Debug test failures ────────────────────────────────────────────
dbt test --store-failures            # Lưu rows lỗi vào schema 'dbt_test__audit'
dbt test --select test_price_positive  # Chỉ chạy 1 test cụ thể

# ─── Documentation ─────────────────────────────────────────────────
dbt docs generate                    # Tạo HTML docs
dbt docs serve                       # Mở browser xem docs + lineage graph

# ─── Quản lý ────────────────────────────────────────────────────────
dbt clean                           # Xoá thư mục target/ và dbt_packages/
dbt compile                         # Compile SQL (không chạy), kiểm tra syntax
```

---

## 🧪 Danh Sách Tests

### Tests trên Source (`delta.default.gold_ohlcv`)

| Test | File | Kiểm tra |
|------|------|----------|
| `test_no_null_symbol` | singular/ | symbol không NULL, không rỗng |
| `test_no_future_timestamps` | singular/ | window_start <= NOW() + 10 phút |
| `test_price_positive` | singular/ | open/high/low/close > 0, volume >= 0 |
| `test_no_missing_1min_candles` | singular/ | Không gap nến 1-phút (2 giờ gần nhất) |
| `test_ohlcv_unique_candle` | singular/ | Không duplicate (symbol, window_start, window_minutes) |

### Tests trên Model `stg_gold_ohlcv` (schema.yml)

| Cột | Test |
|-----|------|
| symbol | not_null, not_empty_string |
| window_start | not_null |
| window_minutes | not_null, accepted_values [1,5] |
| high | not_null, assert_high_gte_low |
| low, open, close, volume | not_null |

### Tests trên Model `mart_crypto_dashboard` (schema.yml)

| Cột | Test |
|-----|------|
| symbol | not_null |
| window_start, window_end, trade_date | not_null |
| window_minutes | not_null, accepted_values [1,5] |
| high | assert_high_gte_low |
| candle_direction | not_null, accepted_values ['BULLISH','BEARISH'] |

---

## 🔧 Sử Dụng Với Docker

Khi Airflow (Teammate 1) cần trigger dbt sau mỗi Gold job:

```bash
# Thêm service vào docker-compose.yml (xem bên dưới)
docker-compose --profile dbt run dbt
```

Snippet cần thêm vào `docker-compose.yml`:

```yaml
  dbt:
    image: ghcr.io/dbt-labs/dbt-trino:1.8.latest
    container_name: dbt
    profiles: ["dbt"]
    volumes:
      - ./dbt:/usr/app/dbt
      - C:\Users\<YourUsername>\.dbt\profiles.yml:/root/.dbt/profiles.yml:ro
    working_dir: /usr/app/dbt
    command: sh -c "dbt deps && dbt run && dbt test"
    environment:
      DBT_PROFILES_DIR: /root/.dbt
    networks:
      - lakehouse-net
    depends_on:
      - trino
```

---

## ❓ Câu Hỏi Thường Gặp

**Q: `dbt debug` báo "Could not connect to Trino"?**  
A: Kiểm tra Trino container: `docker ps | grep trino`. Nếu không có → `docker-compose up -d trino`.

**Q: `dbt run` chạy nhưng không thấy view/table trong Trino?**  
A: Vào Trino UI `http://localhost:8080`, chạy: `SHOW SCHEMAS FROM delta;` và `SHOW TABLES FROM delta.marts;`

**Q: Test `test_no_missing_1min_candles` bị timeout?**  
A: Thêm `--exclude test_no_missing_1min_candles` khi chạy, hoặc giảm INTERVAL từ 2 giờ xuống 30 phút.

**Q: Teammate 1 chưa tạo Gold table, test báo lỗi "Table not found"?**  
A: Dùng mock data: `dbt seed` để load `mock_gold_ohlcv.csv`, sau đó update `sources.yml` trỏ sang seed table.
