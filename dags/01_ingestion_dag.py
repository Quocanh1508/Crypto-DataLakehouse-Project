"""
01_ingestion_dag.py
===================
Daily Ingestion: REST API Batch → Kafka → trigger Silver

Schedule : 08:00 UTC daily (15:00 ICT)
Chain    : run_batch_producer → trigger_silver_pipeline

Design Decisions:
  - validate_kafka   : Removed. Kafka errors surface naturally inside the batch
                       producer itself. No need for a separate check gate.
  - check_streaming  : Removed. WebSocket producer runs as Docker service with
                       restart:always. Health-checking it adds no real benefit.
  - Network name     : Uses docker-compose default network (project-name_lakehouse-net)
  - Volume mount     : Uses ./ingestion mounted at /app inside the container

Teammate 2 (Orchestration) — Fixed hardcoded path + network name.
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
    description="Daily Ingestion: REST API Batch → Kafka → trigger Silver pipeline",
    schedule_interval="0 8 * * *",   # 08:00 UTC daily (15:00 ICT)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ingestion", "batch", "kafka"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Task 1: Run batch producer (REST API klines → Kafka) ──────────────────────
# NOTE: Uses `docker exec` to run inside the existing producer-stream container,
#       which already has Kafka connectivity and all required packages.
#       This avoids spinning up a new container and hardcoding host paths.
run_batch_producer = BashOperator(
    task_id="run_batch_producer",
    bash_command="""
        set -e
        echo "[INGESTION] Starting REST API batch producer..."
        echo "  Fetching top 10 coins historical klines → Kafka"

        # Check if producer-stream container is running
        if ! docker ps --filter "name=producer-stream" --format '{{.Names}}' | grep -q producer-stream; then
            echo "❌ producer-stream container is not running."
            echo "   Run: docker-compose up -d producer-stream"
            exit 1
        fi

        # Run batch producer inside the existing producer-stream container
        # All environment vars (KAFKA_BOOTSTRAP_SERVERS, etc.) are already set
        docker exec -i producer-stream python producer_batch.py

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
