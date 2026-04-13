"""
01_ingestion_dag.py
===================
Daily Ingestion: REST API Batch → Kafka → trigger Silver

Schedule : 08:00 UTC daily (15:00 ICT)
Chain    : run_batch_producer → trigger_silver_pipeline

Removed tasks (vs original):
  - validate_kafka       : Kafka errors will surface naturally inside the batch
                           producer itself. No need for a separate check gate.
  - check_streaming_alive: WebSocket producer runs as Docker service with
                           restart:always. Health-checking it in the pipeline
                           only adds a success gate with no real benefit.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="01_ingestion_dag",
    default_args=default_args,
    description="Daily Ingestion: REST API Batch → Kafka → trigger Silver pipeline",
    schedule_interval="0 8 * * *",   # 08:00 UTC daily (15:00 ICT)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ingestion", "batch", "kafka"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Task 1: Run batch producer (REST API klines → Kafka) ──────────────────────
run_batch_producer = BashOperator(
    task_id="run_batch_producer",
    bash_command="""
        set -e
        echo "[INGESTION] Starting REST API batch producer..."
        echo "  Fetching top 10 coins historical klines → Kafka"

        docker run --rm \
            --network finalproject_lakehouse-net \
            -v "C:/StudyZone/BDAN/FinalProject/ingestion:/app" \
            -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
            -e KAFKA_TOPIC_RAW=crypto_trades_raw \
            -e KAFKA_TOPIC_DLQ=crypto_trades_dlq \
            -e BINANCE_REST_URL=https://api.binance.com \
            -e TOP_N_COINS=10 \
            python:3.10-slim \
            bash -c "pip install -q kafka-python requests && python /app/producer_batch.py"

        echo "✓ Batch ingestion complete → data now in Kafka"
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# ── Task 2: Trigger Silver pipeline ───────────────────────────────────────────
trigger_silver = TriggerDagRunOperator(
    task_id="trigger_silver_pipeline",
    trigger_dag_id="03_silver_dag",
    trigger_rule="all_success",
    dag=dag,
)

# ── Dependencies ───────────────────────────────────────────────────────────────
run_batch_producer >> trigger_silver
