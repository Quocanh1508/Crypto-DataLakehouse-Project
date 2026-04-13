"""
01_ingestion_dag.py
===================
Daily Ingestion Orchestration: REST API Batch → Kafka → trigger Silver

Schedule : 08:00 UTC daily
Pattern  : Follows the same BashOperator + TriggerDagRunOperator pattern
           as 03_silver_dag.py and 04_gold_dag.py

Chain:
  01_ingestion_dag (08:00 UTC)
      ├─ validate_kafka          → check Kafka broker is reachable
      ├─ check_streaming_alive   → verify WebSocket producer is running (warn only)
      ├─ run_batch_producer      → REST API klines → Kafka (producer_batch.py)
      └─ trigger_silver          → trigger 03_silver_dag (09:00 UTC offset built-in)

NOTE: The WebSocket producer (producer_stream.py) runs 24/7 as a Docker Compose
service with restart:always — it does NOT need an Airflow task to start it.
This DAG only health-checks it for visibility.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="01_ingestion_dag",
    default_args=default_args,
    description="Daily Ingestion: REST API Batch → Kafka, then trigger Silver pipeline",
    schedule_interval="0 8 * * *",   # 08:00 UTC daily
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ingestion", "batch", "kafka"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Task 1: Validate Kafka is reachable ──────────────────────────────────────
validate_kafka = BashOperator(
    task_id="validate_kafka",
    bash_command="""
        set -e
        echo "[INGESTION] Validating Kafka connectivity..."
        docker exec spark-master bash -c 'timeout 10 bash -c "cat < /dev/null > /dev/tcp/kafka/29092"' || \
        (echo "❌ Kafka unreachable"; exit 1)
        echo "✓ Kafka OK"
    """,
    dag=dag,
)

# ── Task 2: Check WebSocket streaming producer is alive (warn only) ──────────
# Since producer-stream runs via Docker Compose with restart:always,
# this task is informational — it warns but does NOT block the pipeline.
check_streaming_alive = BashOperator(
    task_id="check_streaming_alive",
    bash_command="""
        echo "[INGESTION] Checking WebSocket streaming producer status..."
        if docker inspect --format='{{.State.Status}}' producer-stream 2>/dev/null | grep -q "running"; then
            echo "✓ producer-stream container is RUNNING (WebSocket 24/7 active)"
        else
            echo "⚠️  WARNING: producer-stream container is NOT running."
            echo "   Batch ingestion will still proceed, but realtime ticks may be missing."
            echo "   Run: docker-compose up -d producer-stream"
        fi
    """,
    # Never fail this task — it's a health check, not a hard dependency
    dag=dag,
)

# ── Task 3: Run batch producer (REST API klines → Kafka) ─────────────────────
run_batch_producer = BashOperator(
    task_id="run_batch_producer",
    bash_command="""
        set -e
        echo "[INGESTION] Starting REST API batch producer..."
        echo "  Fetching top 10 coins historical klines → Kafka"
        echo ""

        docker exec -i \
            -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
            -e KAFKA_TOPIC_RAW=crypto_trades_raw \
            -e KAFKA_TOPIC_DLQ=crypto_trades_dlq \
            -e BINANCE_REST_URL=https://api.binance.com \
            -e TOP_N_COINS=10 \
            airflow-webserver \
            python /home/airflow/processing/../ingestion/producer_batch.py

        echo "✓ Batch ingestion complete → data now in Kafka"
    """,
    # Allow up to 30 min for 10 symbols × 1000 klines each
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# ── Task 4: Trigger Silver pipeline ──────────────────────────────────────────
# Exactly the same pattern as DAG 03 → 04 → 03 chain
trigger_silver = TriggerDagRunOperator(
    task_id="trigger_silver_pipeline",
    trigger_dag_id="03_silver_dag",
    trigger_rule="all_success",
    dag=dag,
)

# ── Dependencies ──────────────────────────────────────────────────────────────
# validate_kafka runs first
# check_streaming_alive runs in parallel with kafka check (informational)
# batch producer waits for kafka validation only
# silver is triggered after batch is done
validate_kafka >> run_batch_producer
check_streaming_alive >> run_batch_producer
run_batch_producer >> trigger_silver
