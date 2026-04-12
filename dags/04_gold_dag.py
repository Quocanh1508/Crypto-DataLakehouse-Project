"""
04_gold_dag.py
==============
Gold Layer: Silver → GCS Delta Lake (OHLCV + Indicators)

Trigger: Triggered after Silver DAG completes
Purpose: Aggregate to OHLCV + add technical indicators
Input: gs://crypto-lakehouse-group8/silver/crypto_ticks
Output: gs://crypto-lakehouse-group8/gold/ohlcv_1m & indicators
Processing:
  - Aggregate trades to 1m OHLCV candles
  - Calculate MA7, MA20, MA50
  - Add technical indicators

CLOSED LOOP: After Gold completes, trigger Silver again
  Gold.final_trigger_silver → TriggerDagRunOperator(03_silver_dag)
  └─ Creates feedback loop: Silver → Gold → Silver → ...
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="04_gold_dag",
    default_args=default_args,
    description="Silver → Gold (OHLCV + Indicators) with self-loop via Silver",
    schedule_interval=None,  # Triggered by Silver
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["gold", "aggregation"],
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ── Wait for Silver ───────────────────────────────────────────────────────────
wait_silver = ExternalTaskSensor(
    task_id="wait_for_silver_completion",
    external_dag_id="03_silver_dag",
    external_task_id="verify_silver_output",
    poke_interval=30,  # Check every 30 seconds
    timeout=1800,  # Wait up to 30 minutes
    mode="poke",
    dag=dag,
)

# ── Run Silver to Gold ────────────────────────────────────────────────────────
run_gold = BashOperator(
    task_id="silver_to_gold_aggregation",
    bash_command="""
        set -e
        echo "[GOLD] Starting Silver → Gold aggregation..."
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
            /processing/silver_to_gold.py
        
        echo "✓ Gold aggregation completed"
    """,
    dag=dag,
)

# ── Verify output ─────────────────────────────────────────────────────────────
verify = BashOperator(
    task_id="verify_gold_output",
    bash_command="""
        echo "[GOLD] Verifying Gold output..."
        
        python3 << 'EOF'
import subprocess

result = subprocess.run(
    ["gsutil", "ls", "-r", "gs://crypto-lakehouse-group8/gold/ohlcv_1m"],
    capture_output=True,
    text=True
)

if result.returncode == 0 and "part-" in result.stdout:
    print("✓ Gold Delta Lake files detected")
else:
    print("⚠ No output yet")

EOF
    """,
    dag=dag,
)

# ── Loop back to Silver (Chu kì khép kín) ────────────────────────────────────
trigger_silver_next = TriggerDagRunOperator(
    task_id="trigger_silver_next_cycle",
    trigger_dag_id="03_silver_dag",
    trigger_rule="all_done",  # Always trigger, even if previous fails
    dag=dag,
)

wait_silver >> run_gold >> verify >> trigger_silver_next
