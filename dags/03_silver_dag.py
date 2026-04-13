from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="03_silver_dag",
    default_args=default_args,
    description="Bronze → Silver (Deduplicate & Clean) - Self-looping via Gold feedback",
    schedule_interval=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["silver", "batch"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

run_silver = BashOperator(
    task_id="bronze_to_silver_batch",
    bash_command="""
        set -e
        echo "[BRONZE→SILVER] Starting Silver transformation job..."
        echo "✅ Running on Spark Master (1 executor × 1 core)"
        echo ""

        # Prevent duplicate execution (Anti-collision lock)
        if docker exec spark-master pgrep -f "bronze_to_silver.py" >/dev/null; then
            echo "❌ BronzeToSilver is already running."
            echo "   Skipping this run to avoid resource starvation."
            exit 1
        fi

        docker exec -i spark-master spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 512m \
            --executor-memory 512m \
            --num-executors 1 \
            --executor-cores 1 \
            --packages io.delta:delta-spark_2.12:3.2.1 \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            --conf "spark.sql.shuffle.partitions=2" \
            --conf "spark.cores.max=1" \
            /processing/bronze_to_silver.py

        echo "✓ Silver transformation completed"
    """,
    dag=dag,
)

trigger_gold = TriggerDagRunOperator(
    task_id="trigger_gold_next",
    trigger_dag_id="04_gold_dag",
    trigger_rule="all_success",
    dag=dag,
)

run_silver >> trigger_gold