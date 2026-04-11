"""
DAG: Silver → Gold OHLCV Aggregation
Purpose: Schedule the silver_to_gold.py batch job to run hourly
Author: Teammate 1 (Data Orchestrator)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# DAG configuration
default_args = {
    'owner': 'data-orchestrator',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'silver_to_gold_aggregation',
    default_args=default_args,
    description='Transform Silver layer ticks into Gold OHLCV candles with MA indicators',
    schedule_interval='0 * * * *',  # Every hour at :00
    catchup=False,
    tags=['gold-layer', 'ohlcv', 'production'],
)

# ─── TASK 1: Gold Layer Aggregation ────────────────────────────────────────────
run_silver_to_gold = BashOperator(
    task_id='run_silver_to_gold',
    bash_command="""
    set -e
    
    echo "=========================================="
    echo "🚀 Starting Silver → Gold Aggregation"
    echo "=========================================="
    echo "Start time: $(date)"
    
    docker exec -e COMPUTE_MOVING_AVERAGES="true" \
                 -e SPARK_MASTER_URL="spark://spark-master:7077" \
        spark-master spark-submit \
            --packages io.delta:delta-spark_2.12:3.2.1,com.google.cloud.bigdataoss:gcs-connector:3.0.0 \
            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
            --conf spark.driver.memory=1g \
            --conf spark.executor.memory=1500m \
            --conf spark.executor.cores=2 \
            --conf spark.sql.shuffle.partitions=8 \
            --conf spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore \
            /processing/silver_to_gold.py
    
    echo "End time: $(date)"
    echo "✅ Gold layer aggregation completed successfully!"
    """,
    dag=dag,
)

# ─── DAG Execution ─────────────────────────────────────────────────────────────
# Linear execution (only one task)
run_silver_to_gold
