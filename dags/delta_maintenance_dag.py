"""
DAG: Delta Lake Maintenance
Purpose: Optimize and vacuum Delta Lake tables to compress files and save GCS storage costs
Author: Teammate 1 (Data Orchestrator)
Schedule: Nightly at 2 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# DAG configuration
default_args = {
    'owner': 'data-orchestrator',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'delta_lake_maintenance',
    default_args=default_args,
    description='Optimize and VACUUM Delta tables to save storage costs',
    schedule_interval='0 2 * * *',  # Every day at 02:00 UTC
    catchup=False,
    tags=['maintenance', 'delta-lake', 'cost-optimization'],
)

# ─── Python script for Delta maintenance ───────────────────────────────────────
MAINTENANCE_SCRIPT = """
import logging
import os
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("delta_maintenance")

# Spark configuration
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
        # OPTIMIZE: Compact small files into larger files (Z-order)
        log.info(f"🔧 Running OPTIMIZE on {layer_name}...")
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (symbol, event_time)")
        log.info(f"✅ OPTIMIZE completed for {layer_name}")
        
        # VACUUM: Remove files older than 7 days (not yet used by any timestamp)
        log.info(f"🧹 Running VACUUM on {layer_name}...")
        spark.sql(f"VACUUM delta.`{path}` RETAIN 7 DAYS")
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
"""

# Write the maintenance script to a file in DAG
maintenance_script_path = "/tmp/delta_maintenance.py"

# ─── TASK 1: Write maintenance script ──────────────────────────────────────────
write_script = BashOperator(
    task_id='write_maintenance_script',
    bash_command=f"""
    cat > {maintenance_script_path} << 'EOF'
{MAINTENANCE_SCRIPT}
EOF
    """,
    dag=dag,
)

# ─── TASK 2: Run Delta maintenance ────────────────────────────────────────────
run_maintenance = BashOperator(
    task_id='run_delta_maintenance',
    bash_command=f"""
    set -e
    
    echo "=========================================="
    echo "🧹 Starting Delta Lake Maintenance"
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
            --conf spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore \
            {maintenance_script_path}
    
    echo "End time: $(date)"
    echo "✅ Delta maintenance completed successfully!"
    """,
    dag=dag,
)

# ─── DAG Execution ────────────────────────────────────────────────────────────
write_script >> run_maintenance
