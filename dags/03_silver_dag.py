"""
03_silver_dag.py
================
Silver Layer: Bronze → GCS Delta Lake (Deduplicate & Clean)

Trigger: Manually triggered first time, then self-looping via Gold DAG
Purpose: Clean & deduplicate Bronze data
Input: gs://crypto-lakehouse-group8/bronze/crypto_ticks
Output: gs://crypto-lakehouse-group8/silver/crypto_ticks
Processing:
  - Remove duplicates (by symbol + trade_id)
  - Filter invalid prices/quantities
  - Cast to Decimal(38,18)
  - Add metadata

CLOSED LOOP ARCHITECTURE:
  - 03_silver_dag (runs) → verify → DONE
  - 04_gold_dag (waits) → runs when Silver done → verify → DONE
  - 04_gold_dag.final_trigger_silver → TriggerDagRunOperator
    └─ Kích hoạt 03_silver_dag lại (feedback loop)
    
Mỗi cycle: Silver (2-3 phút) → Gold (2-3 phút) → ~5-6 phút rồi Silver chạy lại
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
    dag_id="03_silver_dag",
    default_args=default_args,
    description="Bronze → Silver (Deduplicate & Clean) - Self-looping via Gold feedback",
    schedule_interval=None,  # Manual trigger first, then auto-triggered by 04_gold_dag
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["silver", "batch"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Run Bronze to Silver ──────────────────────────────────────────────────────
run_silver = BashOperator(
    task_id="bronze_to_silver_batch",
    bash_command="""
        set -e
        echo "[SILVER] Starting Bronze → Silver transformation..."
        echo "✅ Running on Spark Master (1 executor × 2 cores)"
        echo ""
        
        docker exec spark-master spark-submit \
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
            --conf "spark.sql.shuffle.partitions=2" \
            --conf "spark.cores.max=2" \
            /processing/bronze_to_silver.py
        
        echo "✓ Silver transformation completed"
    """,
    dag=dag,
)

# ── Verify output ─────────────────────────────────────────────────────────────
verify = BashOperator(
    task_id="verify_silver_output",
    bash_command="""
        echo "[SILVER] Verifying Silver output..."
        
        python3 << 'EOF'
import subprocess

result = subprocess.run(
    ["gsutil", "ls", "-r", "gs://crypto-lakehouse-group8/silver/crypto_ticks"],
    capture_output=True,
    text=True
)

if result.returncode == 0 and "part-" in result.stdout:
    print("✓ Silver Delta Lake files detected")
else:
    print("⚠ No output yet")

EOF
    """,
    dag=dag,
)

run_silver >> verify
