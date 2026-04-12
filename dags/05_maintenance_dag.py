"""
05_maintenance_dag.py
=====================
Maintenance: OPTIMIZE & VACUUM all Delta Lake tables

Trigger: Daily @ 2 AM UTC (before 3 AM batch)
Purpose: Compact files + remove old versions
Tasks: OPTIMIZE + VACUUM for Bronze, Silver, Gold
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="05_delta_lake_maintenance",
    default_args=default_args,
    description="Nightly Delta Lake OPTIMIZE & VACUUM maintenance",
    schedule_interval="0 2 * * *",  # 2 AM UTC daily
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["maintenance", "delta-lake"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

BRONZE = "gs://crypto-lakehouse-group8/bronze/crypto_ticks"
SILVER = "gs://crypto-lakehouse-group8/silver/crypto_ticks"
GOLD = "gs://crypto-lakehouse-group8/gold/ohlcv_1m"

# ── OPTIMIZE Bronze ───────────────────────────────────────────────────────────
optimize_bronze = BashOperator(
    task_id="optimize_bronze",
    bash_command=f"""
        echo "[MAINTENANCE] Optimizing Bronze..."
        echo "Distributed: 2 executors × 2 cores"
        
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 512m \
            --executor-memory 1g \
            --num-executors 2 \
            --executor-cores 2 \
            --packages io.delta:delta-spark_2.12:3.2.1 \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            --conf "spark.sql.shuffle.partitions=4" \
            - << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("OPTIMIZE delta.`{BRONZE}` ZORDER BY (symbol, processing_date)")
EOF

        echo "✓ Bronze OPTIMIZE done"
    """,
    dag=dag,
)

# ── OPTIMIZE Silver ───────────────────────────────────────────────────────────
optimize_silver = BashOperator(
    task_id="optimize_silver",
    bash_command=f"""
        echo "[MAINTENANCE] Optimizing Silver..."
        echo "Distributed: 2 executors × 2 cores"
        
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 512m \
            --executor-memory 1g \
            --num-executors 2 \
            --executor-cores 2 \
            --packages io.delta:delta-spark_2.12:3.2.1 \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            --conf "spark.sql.shuffle.partitions=4" \
            - << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("OPTIMIZE delta.`{SILVER}` ZORDER BY (symbol, dt)")
EOF

        echo "✓ Silver OPTIMIZE done"
    """,
    dag=dag,
)

# ── OPTIMIZE Gold ────────────────────────────────────────────────────────────
optimize_gold = BashOperator(
    task_id="optimize_gold",
    bash_command=f"""
        echo "[MAINTENANCE] Optimizing Gold..."
        echo "Distributed: 2 executors × 2 cores"
        
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 512m \
            --executor-memory 1g \
            --num-executors 2 \
            --executor-cores 2 \
            --packages io.delta:delta-spark_2.12:3.2.1 \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            --conf "spark.sql.shuffle.partitions=4" \
            - << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("OPTIMIZE delta.`{GOLD}` ZORDER BY (symbol, dt)")
EOF

        echo "✓ Gold OPTIMIZE done"
    """,
    dag=dag,
)

# ── VACUUM Bronze ────────────────────────────────────────────────────────────
vacuum_bronze = BashOperator(
    task_id="vacuum_bronze",
    bash_command=f"""
        echo "[MAINTENANCE] Vacuuming Bronze (7-day retention)..."
        echo "Distributed: 2 executors × 2 cores"
        
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 512m \
            --executor-memory 1g \
            --num-executors 2 \
            --executor-cores 2 \
            --packages io.delta:delta-spark_2.12:3.2.1 \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            --conf "spark.sql.shuffle.partitions=4" \
            - << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("VACUUM delta.`{BRONZE}` RETAIN 168 HOURS")
EOF

        echo "✓ Bronze VACUUM done"
    """,
    dag=dag,
)

# ── VACUUM Silver ────────────────────────────────────────────────────────────
vacuum_silver = BashOperator(
    task_id="vacuum_silver",
    bash_command=f"""
        echo "[MAINTENANCE] Vacuuming Silver (7-day retention)..."
        echo "Distributed: 2 executors × 2 cores"
        
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 512m \
            --executor-memory 1g \
            --num-executors 2 \
            --executor-cores 2 \
            --packages io.delta:delta-spark_2.12:3.2.1 \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            --conf "spark.sql.shuffle.partitions=4" \
            - << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("VACUUM delta.`{SILVER}` RETAIN 168 HOURS")
EOF

        echo "✓ Silver VACUUM done"
    """,
    dag=dag,
)

# ── VACUUM Gold ──────────────────────────────────────────────────────────────
vacuum_gold = BashOperator(
    task_id="vacuum_gold",
    bash_command=f"""
        echo "[MAINTENANCE] Vacuuming Gold (7-day retention)..."
        
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 1g \
            --packages io.delta:delta-spark_2.12:3.2.1 \
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
            - << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("VACUUM delta.`{GOLD}` RETAIN 168 HOURS")
EOF

        echo "✓ Gold VACUUM done"
    """,
    dag=dag,
)

# ── Log completion ────────────────────────────────────────────────────────────
log_done = BashOperator(
    task_id="log_maintenance_complete",
    bash_command="""
        echo "=== Delta Lake Maintenance Complete ==="
        echo "✓ All tables optimized"
        echo "✓ Old versions removed (7-day retention)"
        echo "Next: 3 AM UTC batch producer run"
    """,
    dag=dag,
)

# ── Dependencies ──────────────────────────────────────────────────────────────
# OPTIMIZE tasks run in parallel (no dependency between them)
# Then VACUUM tasks wait for all OPTIMIZE tasks
# Then log_done waits for all VACUUM tasks

vacuum_bronze.set_upstream([optimize_bronze, optimize_silver, optimize_gold])
vacuum_silver.set_upstream([optimize_bronze, optimize_silver, optimize_gold])
vacuum_gold.set_upstream([optimize_bronze, optimize_silver, optimize_gold])

log_done.set_upstream([vacuum_bronze, vacuum_silver, vacuum_gold])
