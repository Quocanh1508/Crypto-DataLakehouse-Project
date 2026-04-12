"""
02_bronze_streaming_dag.py
==========================
Bronze Layer (Streaming): Kafka → GCS Delta Lake

Trigger: Manually started (runs 24/7 continuously)
Purpose: Stream Kafka ticks to Bronze Delta Lake
Input: Kafka topic `crypto_trades_raw`
Output: gs://crypto-lakehouse-group8/bronze/crypto_ticks
Partition: (processing_date, symbol)

This is triggered after producer_stream has data in Kafka.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="02_bronze_streaming_continuous",
    default_args=default_args,
    description="Stream Kafka → Bronze Delta Lake (Continuous)",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["bronze", "streaming"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Validate Kafka ────────────────────────────────────────────────────────────
validate_kafka = BashOperator(
    task_id="validate_kafka",
    bash_command="""
        echo "[BRONZE-STREAMING] Validating Kafka connectivity..."
        timeout 10 bash -c 'cat < /dev/null > /dev/tcp/kafka/29092' || \
        (echo "❌ Kafka unreachable"; exit 1)
        echo "✓ Kafka OK"
    """,
    dag=dag,
)

# ── Run Bronze Streaming ──────────────────────────────────────────────────────
run_bronze = BashOperator(
    task_id="kafka_to_bronze_streaming",
    bash_command="""
        set -e
        echo "[BRONZE-STREAMING] Starting Kafka → Bronze streaming job (BACKGROUND)..."
        echo "✅ Running via docker exec on spark-master"
        echo "   Resource: 1 executor × 2 cores = 2 cores"
        echo "   Mode: Background indefinitely (detached from DAG)"
        echo ""
        
        # Launch in BACKGROUND - Spark job runs 24/7 independent of DAG task
        # nohup ensures process survives even if docker exec exits
        docker exec -e KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
            -e KAFKA_TOPIC_RAW="crypto_trades_raw" \
            spark-master bash -c 'nohup spark-submit \
            --master spark://spark-master:7077 \
            --driver-memory 512m \
            --executor-memory 1g \
            --num-executors 1 \
            --executor-cores 2 \
            --packages "io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,commons-pool:commons-pool:1.6" \
            --exclude-packages "com.google.code.findbugs:jsr305" \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            --conf "spark.streaming.kafka.maxRatePerPartition=10000" \
            --conf "spark.sql.shuffle.partitions=2" \
            --conf "spark.cores.max=2" \
            --conf "spark.ivy.cache.dir=/tmp/ivy" \
            /processing/bronze_streaming.py > /tmp/bronze_streaming.log 2>&1 &' &
        
        sleep 2  # Give docker exec time to launch background process
        echo "✓ Bronze streaming job launched in background"
    """,
    dag=dag,
)

# No verification task - Bronze streaming runs indefinitely until manually stopped

validate_kafka >> run_bronze
