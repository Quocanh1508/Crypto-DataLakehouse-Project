#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Bản ghi chi tiết công việc Teammate 2 — Platform Reliability Engineer
(Cloud migration & metrics): HMS/Trino GCS, đồng bộ catalog, Monitoring.

Chạy từ thư mục gốc repo:
  python -m scripts.task_report.teammate2_detailed_log
  python -m scripts.task_report.teammate2_detailed_log --output reports/output/teammate2_platform_reliability_detail.txt
  python -m scripts.task_report.teammate2_detailed_log --pdf
  python -m scripts.task_report.teammate2_detailed_log --pdf --output-pdf reports/output/teammate2_platform_reliability_detail.pdf

Cần: pip install -r scripts/task_report/requirements-report.txt

File đầu ra: .txt UTF-8; thêm --pdf để xuất .pdf (ReportLab, font Unicode).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def build_detailed_log_text() -> str:
    """Nội dung đầy đủ: ngữ cảnh vai trò, Task 1–3, chi tiết file/cấu hình monitoring."""
    return """================================================================================
BẢN GHI CHI TIẾT — TEAMMATE 2: PLATFORM RELIABILITY ENGINEER
Focus: Cloud migration & metrics (HMS/Trino GCS, catalog, Prometheus/Grafana)
================================================================================
Phạm vi: mô tả những gì đã được triển khai trong repository và Docker stack,
không mô tả thao tác git (pull/merge/rebase).

--------------------------------------------------------------------------------
PHẦN A — BỐI CẢNH VÀ MỤC TIÊU
--------------------------------------------------------------------------------
• Vai trò: đảm bảo độ tin cậy nền tảng khi chuyển lakehouse khỏi MinIO local sang
  Google Cloud Storage (GCS), đồng bộ metadata với Hive Metastore (HMS) dùng
  PostgreSQL làm DB nội bộ, và quan sát hệ thống (Kafka lag, Spark JVM) để
  tránh trễ xử lý và OOM.

• Mục tiêu chính (khớp assignment):
  (1) HMS & Trino trỏ GCS thay vì MinIO hardcode.
  (2) Sau migration: đăng ký bảng Delta bronze/silver vào catalog để công cụ
      downstream (Trino, Spark) thấy được bảng.
  (3) Monitoring: lag consumer Kafka (so với tốc độ vào từ Binance/WebSocket
      pipeline) và bộ nhớ JVM Spark qua JMX để chủ động trước khi crash OOM.

--------------------------------------------------------------------------------
PHẦN B — TASK 1: HMS & TRINO — MIGRATION SANG GCS (CRITICAL PATH)
--------------------------------------------------------------------------------
Đã làm trong code:

1) Warehouse & đường dẫn lake
   • Chuyển warehouse mặc định sang bucket GCS nhóm, ví dụ khu vực bronze dưới
     gs://crypto-lakehouse-group8/… (biến môi trường HIVE_METASTORE_WAREHOUSE_DIR,
     cấu hình hive-site.xml trong image Hive Metastore).
   • HMS không còn phụ thuộc MinIO cho tầng dữ liệu chính; object store là GCS.

2) Hive Metastore (HMS)
   • Image tùy biến (thư mục hive-metastore/): thêm GCS Hadoop connector (jar)
     để HMS đọc/ghi metadata và trỏ được tới gs://.
   • docker-compose: mount service account JSON (GCP) vào container (ví dụ
     GCP_SERVICE_ACCOUNT_KEY → /etc/gcp/sa.json) để HMS/Trino xác thực với GCS.
   • Lưu ý: HMS và Trino dùng file khóa dịch vụ trong container; Spark có thể
     bổ sung ADC qua mount thư mục gcloud / biến GOOGLE_APPLICATION_CREDENTIALS
     tùy pipeline — tách bạch với HMS/Trino.

3) Trino + Delta Lake
   • File trino/catalog/delta.properties: hive.metastore.uri tới HMS,
     hive.gcs.json-key-file-path trỏ file SA, bật delta.register-table-procedure.enabled
     để có thể gọi thủ tục đăng ký bảng Delta.
   • Trino phục vụ catalog delta trên nền metastore đã trỏ GCS.

4) Trino — sửa lỗi cấu hình metrics (ảnh hưởng port host 18080)
   • Trên phiên bản Trino 432, property metrics.enabled=true trong config không
     hợp lệ → container thoát → không listen → trình duyệt báo ERR_CONNECTION_REFUSED
     tại localhost:18080 (map từ 8080 trong container).
   • Đã loại bỏ property không được hỗ trợ để Trino khởi động bình thường; Prometheus
     vẫn scrape /metrics trên cổng nội bộ trino:8080 theo monitoring/prometheus.yml.

5) Spark image (liên quan GCS)
   • spark/Dockerfile: thêm gcs-connector jar vào /opt/spark/jars để job Spark
     đọc/ghi gs:// khi cần.

--------------------------------------------------------------------------------
PHẦN C — TASK 2: ĐỒNG BỘ CATALOG (BRONZE & SILVER) QUA TRINO / DELTA
--------------------------------------------------------------------------------
Đã làm trong code:

• File sql/register_delta_tables.sql: dùng CALL delta.system.register_table để
  đăng ký bảng Delta (ví dụ crypto_trades_bronze, crypto_trades_silver) tại đường
  dẫn GCS tương ứng sau khi đã có dữ liệu Delta thật trên bucket.

• Vận hành: chạy các câu lệnh SQL đó qua Trino khi bucket và đường dẫn đã đúng;
  kiểm tra SHOW TABLES, SELECT COUNT(*) để xác nhận catalog HMS/Trino thấy bảng.

• PostgreSQL trong compose: là DB backend cho Hive Metastore (không thay thế
  “GCS là warehouse” — GCS vẫn là object store; PostgreSQL lưu metadata HMS).

--------------------------------------------------------------------------------
PHẦN D — TASK 3: MONITORING (PROMETHEUS / GRAFANA, KAFKA LAG, SPARK JMX)
--------------------------------------------------------------------------------
D.1 — Prometheus (monitoring/prometheus.yml)

• scrape_configs đã cấu hình các job (trong mạng Docker):
  - trino: trino:8080, metrics_path /metrics
  - kafka-exporter: kafka-exporter:9308 (metric lag/offset consumer group — tùy exporter)
  - postgres: postgres-exporter:9187
  - spark-master: spark-master:9108 (JMX exporter gắn JVM Master)
  - spark-worker: spark-worker:9109 (JMX exporter gắn JVM Worker)
  - prometheus: localhost:9090 (tự giám sát)

D.2 — Kafka & exporter

• docker-compose: dịch vụ kafka-exporter (image kiểu danielqsj/kafka-exporter),
  broker nội bộ thường là kafka:29092; port host cho metric (ví dụ 9308) để Prometheus
  scrape hoặc debug tay.

• Dashboard Grafana (monitoring/grafana/dashboards/lakehouse-kafka-spark.json):
  - Panel offset theo topic: dùng metric từ exporter/broker (tùy query đã gắn).
  - Panel consumer group lag: chỉ có dữ liệu khi có consumer group thực sự đang
    tiêu thụ và exporter expose được lag; nếu chưa chạy Spark streaming consumer
    hoặc không có group tương ứng, panel có thể trống — đó là hạn chế dữ liệu,
    không phải lỗi Grafana.

D.3 — Spark JVM — JMX → Prometheus (chi tiết triển khai)

• spark/Dockerfile:
  - Tải jmx_prometheus_javaagent-0.20.0.jar vào /opt/jmx/jmx_prometheus_javaagent.jar
  - COPY spark/jmx/jmx-config.yaml vào /opt/jmx/jmx-config.yaml

• spark/jmx/jmx-config.yaml:
  - lowercaseOutputName / lowercaseOutputLabelNames
  - whitelistObjectNames cho java.lang:type=Memory và java.lang:type=OperatingSystem
  - rules map HeapMemoryUsage>used → metric jvm_memory_heap_used_bytes;
    NonHeapMemoryUsage>used → jvm_memory_nonheap_used_bytes;
    (và OpenFileDescriptorCount nếu cần quan sát FD)

• spark/start-spark.sh:
  - SPARK_MASTER_OPTS: -javaagent=...=9108:/opt/jmx/jmx-config.yaml
  - SPARK_WORKER_OPTS: -javaagent=...=9109:/opt/jmx/jmx-config.yaml
  - QUAN TRỌNG: export SPARK_NO_DAEMONIZE=1
    Lý do: start-master.sh / start-worker.sh gọi spark-daemon.sh với nohup mặc định;
    PID 1 trong container thoát ngay → container dừng → không còn JVM → không có
    metric JMX. Với SPARK_NO_DAEMONIZE=1, Spark chạy foreground, container sống,
    cổng 9108/9109 phục vụ /metrics.

• docker-compose.yml: publish host 9108 (master), 9109 (worker) cho JMX exporter.

D.4 — Grafana

• Provisioning: monitoring/grafana/provisioning/datasources/prometheus.yaml (Prometheus
  là datasource mặc định); dashboards/dashboard.yml trỏ tới thư mục JSON.

• Dashboard JSON trong monitoring/grafana/dashboards/:
  - lakehouse-kafka-spark.json: Kafka (offset/lag) + Spark heap/non-heap JVM
  - lakehouse-prometheus-targets.json: trạng thái up của target (kiểm tra scrape)
  - lakehouse-overview.json: tổng quan (nếu có trong repo)

D.5 — Bảng port host (tham chiếu nhanh khi kiểm tra tay)

• Kafka 9092, Kafka Connect 8083, PostgreSQL 5432, Hive Metastore 9083,
  Trino 18080 (host map), Spark Master UI 8082, Spark JMX 9108/9109,
  Prometheus 9090, Grafana 3000, Kafka exporter metrics 9308, Postgres exporter 9187,
  Airflow UI 8888 (nếu bật trong compose).

--------------------------------------------------------------------------------
PHẦN E — KẾT LUẬN VẬN HÀNH
--------------------------------------------------------------------------------
• Migration GCS + HMS + Trino Delta: đã gắn trong cấu hình repo và image; cần SA
  hợp lệ và bucket tồn tại khi chạy thật trên GCP.

• Catalog bronze/silver: thực thi register_delta_tables.sql khi dữ liệu Delta đã có.

• Monitoring: Prometheus scrape đủ target; Grafana hiển thị khi series tồn tại.
  Lag Kafka phụ thuộc consumer group; JVM Spark phụ thuộc container Spark chạy và
  JMX exporter hoạt động (đã xử lý SPARK_NO_DAEMONIZE + jmx-config).

• File này được tạo bởi scripts/task_report/teammate2_detailed_log.py — có thể chạy
  lại để ghi đè bản log mới nhất cùng nội dung mô tả triển khai.

================================================================================
"""


def write_log(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    text = build_detailed_log_text()
    output_path.write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ghi file text chi tiết Teammate 2 (platform reliability / monitoring)."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports") / "output" / "teammate2_platform_reliability_detail.txt",
        help="Đường dẫn file .txt đầu ra",
    )
    parser.add_argument(
        "--pdf",
        action="store_true",
        help="Xuất thêm PDF (cùng nội dung chi tiết; mặc định reports/output/teammate2_platform_reliability_detail.pdf)",
    )
    parser.add_argument(
        "--output-pdf",
        type=Path,
        default=None,
        help="Đường dẫn file .pdf (khi dùng --pdf)",
    )
    args = parser.parse_args()
    out = args.output.resolve()
    write_log(out)
    print(f"Đã ghi: {out}", file=sys.stderr)

    if args.pdf:
        from scripts.task_report.pdf_report import build_teammate2_detail_pdf

        if args.output_pdf is not None:
            pdf_path = Path(args.output_pdf).resolve()
        else:
            pdf_path = out.with_suffix(".pdf")
        build_teammate2_detail_pdf(pdf_path)
        print(f"Đã ghi PDF: {pdf_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
