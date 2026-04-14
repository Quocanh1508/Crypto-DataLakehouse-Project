# -*- coding: utf-8 -*-
"""Nội dung text cố định cho PDF (tóm tắt task — khớp TASK_CLOUD_MIGRATION_METRICS)."""

REPORT_TITLE = "Báo cáo: Cloud migration & metrics (Lakehouse)"

INTRO = """Dự án triển khai warehouse Hive Metastore trỏ Google Cloud Storage, Trino với Delta Lake,
đồng bộ catalog bảng bronze/silver, và monitoring qua Prometheus/Grafana cùng exporter Kafka/JMX Spark.
Báo cáo này kèm ảnh chụp màn hình luồng thao tác trên các dịch vụ local (khi stack Docker đang chạy)."""

SECTIONS = [
    (
        "Task 1 — HMS & Trino: migration sang GCS",
        """Warehouse chuyển sang gs://crypto-lakehouse-group8/ (hive-site, biến HIVE_METASTORE_WAREHOUSE_DIR).
Hive Metastore có GCS connector; Trino catalog dùng hive.metastore.uri, Delta Lake, service account JSON.
GCP Console (bucket bronze/silver) và PR: minh chứng bổ sung thủ công nếu cần.""",
    ),
    (
        "Task 2 — Đồng bộ catalog (bronze & silver)",
        """Script sql/register_delta_tables.sql gọi CALL delta.system.register_table cho crypto_trades_bronze và
crypto_trades_silver. Kiểm tra SHOW TABLES / SELECT sau khi có dữ liệu thật trên Delta.""",
    ),
    (
        "Task 3 — Monitoring",
        """Prometheus scrape Trino (/metrics), kafka-exporter (9308), postgres-exporter (9187); Grafana datasource Prometheus (compose: prometheus 9090, grafana 3000, admin/admin).
Dashboard provision: Lakehouse — Prometheus targets; Lakehouse Kafka + Spark. Spark JMX scrape 9108/9109.""",
    ),
]

PORT_TABLE_HEADER = ("Dịch vụ", "Port (host)")
PORT_ROWS = [
    ("Kafka", "9092"),
    ("Kafka Connect", "8083"),
    ("PostgreSQL", "5432"),
    ("Hive Metastore", "9083"),
    ("Trino", "18080"),
    ("Spark Master UI", "8082"),
    ("Spark Master JMX", "9108"),
    ("Spark Worker JMX", "9109"),
    ("Prometheus", "9090"),
    ("Grafana", "3000"),
    ("Kafka Exporter metrics", "9308"),
    ("Postgres exporter", "9187"),
    ("Airflow Web UI", "8888"),
]

MANUAL_PLACEHOLDERS = """Minh chứng thủ công (điền/ghi chú khi nộp báo cáo)
• GCP Console: bucket crypto-lakehouse-group8, thư mục bronze/silver
• Pull Request / git log trên nhánh feature
• docker compose ps (ảnh terminal) để thấy container Up"""
