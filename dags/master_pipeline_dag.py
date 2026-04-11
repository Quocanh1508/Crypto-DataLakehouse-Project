"""
DAG: Master Crypto Lakehouse Pipeline
Purpose: Orchestrate the complete data pipeline from ingestion to analytics
Schedule:
  - producer_batch: Daily @ 8 AM (pull historical REST API data)
  - bronze_to_silver: Daily @ 9 AM (clean & deduplicate)
  - silver_to_gold: Hourly @ :00 (aggregate OHLCV + MA in realtime)
  - delta_maintenance: Nightly @ 2 AM (optimize & vacuum)
Author: Teammate 1 (Data Orchestrator)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

log = logging.getLogger(__name__)

# ─── DAG Configuration ─────────────────────────────────────────────────────────
default_args = {
    'owner': 'data-orchestrator',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# ─── MAIN PIPELINE DAG (Daily orchestration) ───────────────────────────────────
dag_main = DAG(
    'master_crypto_pipeline_daily',
    default_args=default_args,
    description='Master DAG: producer_batch → bronze_to_silver → delta_maintenance',
    schedule_interval='0 8 * * *',  # Every day @ 8 AM UTC
    catchup=False,
    tags=['master', 'production', 'daily'],
)

# ─── REALTIME AGGREGATION DAG (Silver → Gold every 15 minutes) ──────────────
dag_realtime = DAG(
    'silver_to_gold_realtime',
    default_args={
        **default_args,
        'start_date': days_ago(0),  # Start immediately
    },
    description='Every 15 minutes: Transform Silver ticks into Gold OHLCV candles (realtime)',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    tags=['gold-layer', 'realtime', '15min'],
)

# ═══════════════════════════════════════════════════════════════════════════════
# ─── TASK 1: Producer Batch (REST API → GCS raw-batch) ────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
task_producer_batch = BashOperator(
    task_id='producer_batch_rest_api',
    bash_command="""
    set -e
    
    echo "=========================================="
    echo "📥 Starting Producer Batch (REST API)"
    echo "=========================================="
    echo "Start time: $(date)"
    echo "Pulling historical data from Binance REST API..."
    
    docker exec -e BATCH_SIZE="1000" \
                 -e LOOKBACK_DAYS="30" \
        spark-master python3 /processing/producer_batch.py
    
    echo "End time: $(date)"
    echo "✅ Producer batch completed!"
    """,
    dag=dag_main,
)

# ═══════════════════════════════════════════════════════════════════════════════
# ─── TASK 2: Bronze to Silver (Merge + Deduplicate) ───────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
task_bronze_to_silver = BashOperator(
    task_id='bronze_to_silver_transform',
    bash_command="""
    set -e
    
    echo "=========================================="
    echo "🔄 Starting Bronze → Silver Transformation"
    echo "=========================================="
    echo "Start time: $(date)"
    
    docker exec -e SPARK_MASTER_URL="spark://spark-master:7077" \
        spark-master spark-submit \
            --packages io.delta:delta-spark_2.12:3.2.1,com.google.cloud.bigdataoss:gcs-connector:3.0.0 \
            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
            --conf spark.driver.memory=1g \
            --conf spark.executor.memory=1500m \
            --conf spark.executor.cores=2 \
            --conf spark.sql.shuffle.partitions=8 \
            --conf spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore \
            /processing/bronze_to_silver.py
    
    echo "End time: $(date)"
    echo "✅ Bronze to Silver completed!"
    """,
    dag=dag_main,
)

# ═══════════════════════════════════════════════════════════════════════════════
# ─── TASK 3: Silver to Gold (OHLCV Aggregation - HOURLY) ──────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
task_silver_to_gold = BashOperator(
    task_id='silver_to_gold_aggregation',
    bash_command="""
    set -e
    
    echo "=========================================="
    echo "⭐ Starting Silver → Gold Aggregation"
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
    echo "✅ Gold layer aggregation completed!"
    """,
    dag=dag_realtime,
)

# ═══════════════════════════════════════════════════════════════════════════════
# ─── TASK 4: Delta Maintenance (OPTIMIZE + VACUUM) ────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
task_delta_maintenance = BashOperator(
    task_id='delta_lake_maintenance',
    bash_command="""
    set -e
    
    echo "=========================================="
    echo "🧹 Starting Delta Lake Maintenance"
    echo "=========================================="
    echo "Start time: $(date)"
    
    cat > /tmp/delta_maintenance.py << 'EOF'
import logging
import os
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("delta_maintenance")

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
GCS_PATHS = {
    'bronze': 'gs://crypto-lakehouse-group8/bronze',
    'silver': 'gs://crypto-lakehouse-group8/silver',
    'gold': 'gs://crypto-lakehouse-group8/gold',
}

def create_spark():
    return SparkSession.builder \\
        .appName("DeltaMaintenance") \\
        .master(SPARK_MASTER) \\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore") \\
        .getOrCreate()

def optimize_and_vacuum(spark, layer_name, path):
    log.info(f"Starting maintenance on {layer_name} layer: {path}")
    try:
        log.info(f"🔧 Running OPTIMIZE on {layer_name}...")
        spark.sql(f"OPTIMIZE delta.\\`{path}\\` ZORDER BY (symbol, event_time)")
        log.info(f"✅ OPTIMIZE completed for {layer_name}")
        
        log.info(f"🧹 Running VACUUM on {layer_name}...")
        spark.sql(f"VACUUM delta.\\`{path}\\` RETAIN 7 DAYS")
        log.info(f"✅ VACUUM completed for {layer_name}")
    except Exception as e:
        log.error(f"❌ Maintenance failed for {layer_name}: {str(e)}")
        raise

def main():
    spark = create_spark()
    try:
        log.info("=" * 80)
        log.info("DELTA LAKE MAINTENANCE: OPTIMIZE & VACUUM")
        log.info("=" * 80)
        
        for layer, path in GCS_PATHS.items():
            optimize_and_vacuum(spark, layer, path)
        
        log.info("=" * 80)
        log.info("✅ All Delta tables maintained successfully!")
        log.info("=" * 80)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
EOF
    
    docker exec -e SPARK_MASTER_URL="spark://spark-master:7077" \
        spark-master spark-submit \
            --packages io.delta:delta-spark_2.12:3.2.1,com.google.cloud.bigdataoss:gcs-connector:3.0.0 \
            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
            --conf spark.driver.memory=1g \
            --conf spark.executor.memory=1500m \
            --conf spark.executor.cores=2 \
            --conf spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore \
            /tmp/delta_maintenance.py
    
    echo "End time: $(date)"
    echo "✅ Delta maintenance completed!"
    """,
    dag=dag_main,
)

# ═══════════════════════════════════════════════════════════════════════════════
# ─── DAG Task Dependencies ─────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

# MAIN DAG: Daily pipeline
task_producer_batch >> task_bronze_to_silver >> task_delta_maintenance

# HOURLY DAG: Silver → Gold realtime
task_silver_to_gold
