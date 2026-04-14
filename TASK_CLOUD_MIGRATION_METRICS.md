# Task: Cloud migration & metrics (Platform Reliability)

Tóm tắt **nội dung công việc đã triển khai trong code** — khớp với các mục HMS/Trino GCS, đồng bộ catalog, và monitoring. Không mô tả thao tác git (pull/merge).

---

## Task 1 — HMS & Trino: migration sang GCS (critical path)

| Hạng mục | Đã làm |
|----------|--------|
| **Warehouse** | Chuyển từ MinIO/S3 local sang **`gs://crypto-lakehouse-group8/bronze/`** (biến `HIVE_METASTORE_WAREHOUSE_DIR`, `hive-site.xml`, env) |
| **Hive Metastore** | Build image `hive-metastore/` — thêm **GCS Hadoop connector** (jar), HMS đọc/ghi metadata trỏ GCS |
| **docker-compose** | HMS + Trino mount file SA: `GCP_SERVICE_ACCOUNT_KEY` → `/etc/gcp/sa.json`; bỏ phụ thuộc MinIO cho tầng lakehouse chính |
| **Trino Delta** | `trino/catalog/delta.properties`: `hive.gcs.json-key-file-path`, `hive.metastore.uri`, bật `delta.register-table-procedure.enabled` |
| **Auth** | HMS/Trino dùng **service account JSON** trong container (không dùng ADC cho hai service này). Spark trong compose có thể dùng **ADC** qua mount `%APPDATA%/gcloud` + `GOOGLE_APPLICATION_CREDENTIALS` — tách với HMS/Trino |

---

## Task 2 — Trino catalog / đồng bộ metastore (bronze & silver)

| Hạng mục | Đã làm |
|----------|--------|
| **Script** | `sql/register_delta_tables.sql` — gọi `CALL delta.system.register_table` cho **`crypto_trades_bronze`**, **`crypto_trades_silver`** tại đường dẫn GCS tương ứng |
| **Ghi chú vận hành** | Cần chạy SQL khi bucket đã có Delta thật; kiểm tra `SHOW TABLES`, `SELECT` sau khi register |

---

## Task 3 — Monitoring (Prometheus / Grafana, lag Kafka, JMX Spark)

| Hạng mục | Đã làm |
|----------|--------|
| **Prometheus** | `monitoring/prometheus.yml` — scrape **kafka-exporter**, **spark-master** / **spark-worker** (JMX exporter port `9108` trong network Docker) |
| **Kafka lag** | Service **kafka-exporter** (`danielqsj/kafka-exporter`), broker nội bộ `kafka:29092`, port host metrics `9308` |
| **Spark JVM** | **jmx_prometheus_javaagent** trong Spark image + `spark/start-spark.sh`; map host **9108** (master), **9109** (worker) |
| **Grafana** | Image + provisioning datasources & cấu hình dashboard folder; **dashboard JSON chi tiết** có thể bổ sung sau trong `monitoring/grafana/dashboards/` |

### Bảng port (host) phục vụ giám sát

| Dịch vụ | Port |
|--------|------|
| Kafka | 9092 |
| Kafka Connect | 8083 |
| PostgreSQL | 5432 |
| Hive Metastore | 9083 |
| Trino | 18080 (host) |
| Spark Master UI | **8082** |
| Spark Master JMX | 9108 |
| Spark Worker JMX | 9109 |
| Prometheus | 9090 |
| Grafana | 3000 |
| Kafka Exporter | 9308 |

Broker trong Docker: `kafka:29092`. Spark driver UI khi chạy job: thường `http://localhost:4040`.

---

## Việc có thể làm thêm (ngoài phạm vi đã code xong)

- Import hoặc viết **dashboard Grafana** (consumer lag, JVM) nếu cần UI sẵn.
- Chạy và xác nhận `register_table` trên môi trường có dữ liệu thật.

---

*Tài liệu nội bộ — cập nhật khi task thay đổi.*
