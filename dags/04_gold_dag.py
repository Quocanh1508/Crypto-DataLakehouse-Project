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
    dag_id="04_gold_dag",
    default_args=default_args,
    description="Silver → Gold (OHLCV + Indicators) with self-loop via Silver",
    schedule_interval=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["gold", "aggregation"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

run_gold = BashOperator(
    task_id="silver_to_gold_aggregation",
    bash_command="""
        set -e
        echo "[GOLD] Starting Silver → Gold aggregation..."
        echo "✅ Running on Spark Master (1 executor × 2 cores)"
        echo ""

        docker exec -i spark-master spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 1g \
            --executor-memory 2g \
            --num-executors 1 \
            --executor-cores 2 \
            --packages io.delta:delta-spark_2.12:3.2.1 \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            --conf "spark.sql.shuffle.partitions=4" \
            --conf "spark.cores.max=2" \
            /processing/silver_to_gold.py

        echo "✓ Gold aggregation completed"
    """,
    dag=dag,
)

trigger_silver_next = TriggerDagRunOperator(
    task_id="trigger_silver_next_cycle",
    trigger_dag_id="03_silver_dag",
    trigger_rule="all_success",
    dag=dag,
)

run_gold >> trigger_silver_next