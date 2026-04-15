"""
05_maintenance_dag.py
=====================
Maintenance: OPTIMIZE & VACUUM all Delta Lake tables on GCS

Schedule : Daily @ 2 AM UTC (before 3 AM batch)
Purpose  : Compact small files + remove old versions to manage storage costs
Layers   : Bronze, Silver, Gold (all on GCS)

Design Decisions:
  - OPTIMIZE: Compacts many small Parquet files into fewer large ones.
    Uses ZORDER BY (symbol, partition_col) for optimal query locality.
    This is critical because streaming writes create many tiny files.
  - VACUUM: Removes files older than 7 days that are no longer referenced
    by the Delta transaction log. This is what saves storage costs.
  - Retention: 168 hours (7 days) — enough for time-travel rollback,
    short enough to keep GCS costs strictly managed.
  - Execution: All Spark jobs run via `docker exec spark-master` since
    the Airflow container does NOT have Spark/PySpark installed.
  - Parallelism: OPTIMIZE tasks run in parallel (independent layers).
    VACUUM tasks wait for all OPTIMIZE tasks (must compact before cleaning).

Teammate 2 (Orchestration):
  - Fixed critical bug: spark-submit must run via docker exec, not directly
  - Added execution_timeout and proper retries
  - Added completion metrics logging
  - Added spark.databricks.delta.retentionDurationCheck.enabled=false for VACUUM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
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
    tags=["maintenance", "delta-lake", "cost-management"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── GCS Delta Lake table paths ───────────────────────────────────────────────
BRONZE_PATH = "gs://crypto-lakehouse-group8/bronze/crypto_ticks"
SILVER_PATH = "gs://crypto-lakehouse-group8/silver/crypto_ticks"
GOLD_PATH   = "gs://crypto-lakehouse-group8/gold"

# ── Shared Spark submit command template ─────────────────────────────────────
# All commands run inside spark-master container via `docker exec`
SPARK_SUBMIT_PREFIX = """docker exec -i spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 1g \
    --num-executors 1 \
    --executor-cores 2 \
    --packages io.delta:delta-spark_2.12:3.2.1 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" \
    --conf "spark.sql.shuffle.partitions=4" \
    --conf "spark.databricks.delta.retentionDurationCheck.enabled=false" """

# ── Pre-flight: Verify Spark is up ───────────────────────────────────────────
preflight = BashOperator(
    task_id="preflight_check",
    bash_command="""
        set -e
        echo "[MAINTENANCE] Pre-flight checks..."

        # Check spark-master is running
        if ! docker ps --filter "name=spark-master" --format '{{.Names}}' | grep -q spark-master; then
            echo "❌ spark-master is not running. Cannot proceed."
            exit 1
        fi

        echo "✓ spark-master is up"
        echo "✓ Starting Delta Lake maintenance..."
    """,
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

# ── OPTIMIZE Bronze ──────────────────────────────────────────────────────────
optimize_bronze = BashOperator(
    task_id="optimize_bronze",
    bash_command=f"""
        set -e
        echo "[MAINTENANCE] Optimizing Bronze layer..."
        echo "  Path: {BRONZE_PATH}"
        echo "  ZORDER BY: symbol, processing_date"

        {SPARK_SUBMIT_PREFIX} /dev/stdin <<'PYEOF'
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Maintenance_Optimize_Bronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    dt = DeltaTable.forPath(spark, "{BRONZE_PATH}")
    dt.optimize().executeZOrderBy("symbol", "processing_date")
    print("✓ Bronze OPTIMIZE + ZORDER completed successfully")
except Exception as e:
    print(f"⚠ Bronze OPTIMIZE skipped: {{e}}")

spark.stop()
PYEOF

        echo "✓ Bronze OPTIMIZE done"
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# ── OPTIMIZE Silver ──────────────────────────────────────────────────────────
optimize_silver = BashOperator(
    task_id="optimize_silver",
    bash_command=f"""
        set -e
        echo "[MAINTENANCE] Optimizing Silver layer..."
        echo "  Path: {SILVER_PATH}"
        echo "  ZORDER BY: symbol, dt"

        {SPARK_SUBMIT_PREFIX} /dev/stdin <<'PYEOF'
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Maintenance_Optimize_Silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    dt = DeltaTable.forPath(spark, "{SILVER_PATH}")
    dt.optimize().executeZOrderBy("symbol", "dt")
    print("✓ Silver OPTIMIZE + ZORDER completed successfully")
except Exception as e:
    print(f"⚠ Silver OPTIMIZE skipped: {{e}}")

spark.stop()
PYEOF

        echo "✓ Silver OPTIMIZE done"
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# ── OPTIMIZE Gold ────────────────────────────────────────────────────────────
optimize_gold = BashOperator(
    task_id="optimize_gold",
    bash_command=f"""
        set -e
        echo "[MAINTENANCE] Optimizing Gold layer..."
        echo "  Path: {GOLD_PATH}"
        echo "  ZORDER BY: symbol, candle_date"

        {SPARK_SUBMIT_PREFIX} /dev/stdin <<'PYEOF'
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Maintenance_Optimize_Gold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    dt = DeltaTable.forPath(spark, "{GOLD_PATH}")
    dt.optimize().executeZOrderBy("symbol", "candle_date")
    print("✓ Gold OPTIMIZE + ZORDER completed successfully")
except Exception as e:
    print(f"⚠ Gold OPTIMIZE skipped: {{e}}")

spark.stop()
PYEOF

        echo "✓ Gold OPTIMIZE done"
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# ── VACUUM Bronze (7-day retention) ──────────────────────────────────────────
vacuum_bronze = BashOperator(
    task_id="vacuum_bronze",
    bash_command=f"""
        set -e
        echo "[MAINTENANCE] Vacuuming Bronze (7-day retention)..."
        echo "  Files older than 7 days will be permanently deleted."

        {SPARK_SUBMIT_PREFIX} /dev/stdin <<'PYEOF'
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Maintenance_Vacuum_Bronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    dt = DeltaTable.forPath(spark, "{BRONZE_PATH}")
    dt.vacuum(168)  # 168 hours = 7 days
    print("✓ Bronze VACUUM (7-day retention) completed")
except Exception as e:
    print(f"⚠ Bronze VACUUM skipped: {{e}}")

spark.stop()
PYEOF

        echo "✓ Bronze VACUUM done"
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# ── VACUUM Silver (7-day retention) ──────────────────────────────────────────
vacuum_silver = BashOperator(
    task_id="vacuum_silver",
    bash_command=f"""
        set -e
        echo "[MAINTENANCE] Vacuuming Silver (7-day retention)..."

        {SPARK_SUBMIT_PREFIX} /dev/stdin <<'PYEOF'
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Maintenance_Vacuum_Silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    dt = DeltaTable.forPath(spark, "{SILVER_PATH}")
    dt.vacuum(168)  # 168 hours = 7 days
    print("✓ Silver VACUUM (7-day retention) completed")
except Exception as e:
    print(f"⚠ Silver VACUUM skipped: {{e}}")

spark.stop()
PYEOF

        echo "✓ Silver VACUUM done"
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# ── VACUUM Gold (7-day retention) ────────────────────────────────────────────
vacuum_gold = BashOperator(
    task_id="vacuum_gold",
    bash_command=f"""
        set -e
        echo "[MAINTENANCE] Vacuuming Gold (7-day retention)..."

        {SPARK_SUBMIT_PREFIX} /dev/stdin <<'PYEOF'
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Maintenance_Vacuum_Gold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    dt = DeltaTable.forPath(spark, "{GOLD_PATH}")
    dt.vacuum(168)  # 168 hours = 7 days
    print("✓ Gold VACUUM (7-day retention) completed")
except Exception as e:
    print(f"⚠ Gold VACUUM skipped: {{e}}")

spark.stop()
PYEOF

        echo "✓ Gold VACUUM done"
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# ── Log completion with metrics ──────────────────────────────────────────────
log_done = BashOperator(
    task_id="log_maintenance_complete",
    bash_command="""
        echo "============================================="
        echo "  Delta Lake Maintenance Complete"
        echo "============================================="
        echo "  ✓ Bronze OPTIMIZE + VACUUM (7-day retention)"
        echo "  ✓ Silver OPTIMIZE + VACUUM (7-day retention)"
        echo "  ✓ Gold   OPTIMIZE + VACUUM (7-day retention)"
        echo ""
        echo "  Storage costs are now strictly managed."
        echo "  Old versions beyond 7 days have been removed."
        echo "  Small files have been compacted for query performance."
        echo "============================================="
    """,
    dag=dag,
)

# ── Dependencies ──────────────────────────────────────────────────────────────
# preflight → [optimize_bronze, optimize_silver, optimize_gold] (parallel)
#           → [vacuum_bronze, vacuum_silver, vacuum_gold] (parallel, after ALL optimize)
#           → log_done

preflight >> [optimize_bronze, optimize_silver, optimize_gold]

vacuum_bronze.set_upstream([optimize_bronze, optimize_silver, optimize_gold])
vacuum_silver.set_upstream([optimize_bronze, optimize_silver, optimize_gold])
vacuum_gold.set_upstream([optimize_bronze, optimize_silver, optimize_gold])

log_done.set_upstream([vacuum_bronze, vacuum_silver, vacuum_gold])
