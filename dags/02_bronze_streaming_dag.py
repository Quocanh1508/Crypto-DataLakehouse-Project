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
    description="Stream Kafka → Bronze Delta Lake (Continuous, tracked by Airflow)",
    schedule_interval=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["bronze", "streaming"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

validate_kafka = BashOperator(
    task_id="validate_kafka",
    bash_command="""
        set -e
        echo "[BRONZE-STREAMING] Validating Kafka connectivity from spark-master..."
        docker exec spark-master bash -c 'timeout 10 bash -c "cat < /dev/null > /dev/tcp/kafka/29092"' || \
        (echo "❌ Kafka unreachable from spark-master"; exit 1)
        echo "✓ Kafka OK"
    """,
    dag=dag,
)

run_bronze = BashOperator(
    task_id="kafka_to_bronze_streaming",
    bash_command="""
        set -e
        echo "[BRONZE-STREAMING] Starting Kafka → Bronze streaming job (FOREGROUND)..."
        echo "✅ Airflow will track this Spark job directly"
        echo "   Resource: 1 executor × 1 core"
        echo "   Mode: Foreground / supervised by Airflow"
        echo ""

        # Prevent duplicate launches
        if docker exec spark-master pgrep -f "/processing/bronze_streaming.py" >/dev/null; then
            echo "❌ Bronze streaming is already running."
            echo "   Stop the existing process first, or keep only one active stream."
            exit 1
        fi

        # Run in foreground so Airflow can track liveness and failure
        exec docker exec -i \
            -e KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
            -e KAFKA_TOPIC_RAW="crypto_trades_raw" \
            spark-master spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --driver-memory 512m \
                --executor-memory 768m \
                --num-executors 1 \
                --executor-cores 1 \
                --packages "io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,commons-pool:commons-pool:1.6" \
                --exclude-packages "com.google.code.findbugs:jsr305" \
                --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
                --conf "spark.streaming.kafka.maxRatePerPartition=2000" \
                --conf "spark.sql.shuffle.partitions=2" \
                --conf "spark.cores.max=1" \
                --conf "spark.ivy.cache.dir=/tmp/ivy" \
                /processing/bronze_streaming.py
    """,
    dag=dag,
)

validate_kafka >> run_bronze