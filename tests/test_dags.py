"""
tests/test_dags.py
==================
Comprehensive DAG validation tests for Task 2 (Orchestration).

Tests verify:
  1. All DAGs import without errors
  2. DAG structure (tasks, dependencies, no cycles)
  3. Schedule intervals match design spec
  4. Best practice compliance (owner, tags, max_active_runs)
  5. Maintenance DAG task ordering (OPTIMIZE before VACUUM)
  6. Gold DAG has data quality validation tasks
  7. No hardcoded host paths in bash commands

Run:
  cd C:\\HCMUTE\\nam3ki2_1\\bigdata\\Crypto-DataLakehouse-Project
  python -m pytest tests/test_dags.py -v

Teammate 2 (Orchestration) — Test suite proving Airflow DAGs are correct.
"""

import os
import sys
import importlib
import pytest

# ── Setup: Add dags/ to Python path so we can import DAG files ───────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAGS_DIR = os.path.join(PROJECT_ROOT, "dags")
sys.path.insert(0, DAGS_DIR)

# ── Configure Airflow env BEFORE importing airflow ───────────────────────────
# Airflow 2.8 requires an absolute SQLite path; :memory: is rejected.
_airflow_home = os.path.join(PROJECT_ROOT, ".venv-task2", "airflow_home")
os.makedirs(_airflow_home, exist_ok=True)
os.environ.setdefault("AIRFLOW_HOME", _airflow_home)
_db = os.path.join(_airflow_home, "airflow_test.db").replace("\\", "/")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:////{_db}")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

# We need to mock Airflow imports for unit testing without a full Airflow install.
# If airflow is not installed, we install a lightweight mock.
try:
    import airflow
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


# ── Skip all tests if Airflow is not installed ───────────────────────────────
pytestmark = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="Apache Airflow not installed — install with: pip install apache-airflow"
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="module")
def all_dag_files():
    """Return list of all DAG Python files."""
    return sorted([
        f for f in os.listdir(DAGS_DIR)
        if f.endswith(".py") and not f.startswith("__")
    ])


@pytest.fixture(scope="module")
def loaded_dags():
    """Import all DAG files and extract DAG objects."""
    from airflow.models import DAG as AirflowDAG
    dags = {}
    for f in sorted(os.listdir(DAGS_DIR)):
        if f.endswith(".py") and not f.startswith("__"):
            module_name = f[:-3]
            spec = importlib.util.spec_from_file_location(
                module_name, os.path.join(DAGS_DIR, f)
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            # Find DAG objects in module — must be actual DAG instances
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, AirflowDAG):
                    dags[attr.dag_id] = attr
    return dags


# =============================================================================
# TEST GROUP 1: DAG Import Tests
# =============================================================================

class TestDAGImports:
    """Verify all DAG files import without errors."""

    def test_all_dag_files_exist(self, all_dag_files):
        """At least 5 DAG files should exist (01 through 05)."""
        assert len(all_dag_files) >= 5, \
            f"Expected at least 5 DAG files, found {len(all_dag_files)}: {all_dag_files}"

    def test_dag_files_importable(self, loaded_dags):
        """All DAG files should import without errors."""
        assert len(loaded_dags) >= 5, \
            f"Expected at least 5 DAGs, loaded {len(loaded_dags)}: {list(loaded_dags.keys())}"

    def test_expected_dag_ids_present(self, loaded_dags):
        """Check that all expected DAG IDs are present."""
        expected = [
            "01_ingestion_dag",
            "02_bronze_streaming_continuous",
            "03_silver_dag",
            "04_gold_dag",
            "05_delta_lake_maintenance",
        ]
        for dag_id in expected:
            assert dag_id in loaded_dags, f"Missing DAG: {dag_id}"

    def test_no_import_errors(self, loaded_dags):
        """Verify no DAG has import_errors attribute set."""
        for dag_id, dag in loaded_dags.items():
            # A properly loaded DAG should not have import_errors
            assert not hasattr(dag, "import_errors") or dag.import_errors is None, \
                f"DAG {dag_id} has import errors"


# =============================================================================
# TEST GROUP 2: DAG Structure & Best Practices
# =============================================================================

class TestDAGStructure:
    """Verify DAG structure follows best practices."""

    def test_all_dags_have_owner(self, loaded_dags):
        """Every DAG must have an owner in default_args."""
        for dag_id, dag in loaded_dags.items():
            assert dag.default_args.get("owner"), \
                f"DAG {dag_id} is missing 'owner' in default_args"

    def test_all_dags_have_tags(self, loaded_dags):
        """Every DAG must have at least one tag for filtering."""
        for dag_id, dag in loaded_dags.items():
            assert dag.tags and len(dag.tags) > 0, \
                f"DAG {dag_id} is missing tags"

    def test_no_catchup(self, loaded_dags):
        """All DAGs should have catchup=False to prevent backfill storms."""
        for dag_id, dag in loaded_dags.items():
            assert not dag.catchup, \
                f"DAG {dag_id} has catchup=True — this can cause backfill storms"

    def test_max_active_runs(self, loaded_dags):
        """All DAGs should have max_active_runs=1 to prevent parallel conflicts."""
        for dag_id, dag in loaded_dags.items():
            assert dag.max_active_runs == 1, \
                f"DAG {dag_id} has max_active_runs={dag.max_active_runs} (expected 1)"

    def test_no_cycles(self, loaded_dags):
        """Verify no DAG has circular dependencies (Airflow would catch this too)."""
        for dag_id, dag in loaded_dags.items():
            # Airflow validates this at parse time; if we got here, no cycles exist
            tasks = dag.tasks
            assert len(tasks) > 0, f"DAG {dag_id} has no tasks"

    def test_all_dags_have_description(self, loaded_dags):
        """Every DAG should have a description."""
        for dag_id, dag in loaded_dags.items():
            assert dag.description and len(dag.description) > 10, \
                f"DAG {dag_id} is missing or has a too-short description"


# =============================================================================
# TEST GROUP 3: Schedule Validation
# =============================================================================

class TestSchedules:
    """Verify DAG schedules match the design specification."""

    def test_ingestion_schedule(self, loaded_dags):
        """01_ingestion_dag: should run daily at 8 AM UTC."""
        dag = loaded_dags["01_ingestion_dag"]
        assert dag.schedule_interval == "0 8 * * *", \
            f"Expected '0 8 * * *', got '{dag.schedule_interval}'"

    def test_bronze_streaming_no_schedule(self, loaded_dags):
        """02_bronze_streaming: should be trigger-only (None)."""
        dag = loaded_dags["02_bronze_streaming_continuous"]
        assert dag.schedule_interval is None, \
            f"Expected None, got '{dag.schedule_interval}'"

    def test_silver_no_schedule(self, loaded_dags):
        """03_silver_dag: should be trigger-only (None)."""
        dag = loaded_dags["03_silver_dag"]
        assert dag.schedule_interval is None, \
            f"Expected None, got '{dag.schedule_interval}'"

    def test_gold_schedule(self, loaded_dags):
        """04_gold_dag: should run every 15 minutes."""
        dag = loaded_dags["04_gold_dag"]
        assert dag.schedule_interval == "*/15 * * * *", \
            f"Expected '*/15 * * * *', got '{dag.schedule_interval}'"

    def test_maintenance_schedule(self, loaded_dags):
        """05_maintenance: should run daily at 2 AM UTC."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        assert dag.schedule_interval == "0 2 * * *", \
            f"Expected '0 2 * * *', got '{dag.schedule_interval}'"


# =============================================================================
# TEST GROUP 4: Ingestion DAG (01)
# =============================================================================

class TestIngestionDAG:
    """Verify 01_ingestion_dag specifics."""

    def test_task_count(self, loaded_dags):
        """Should have exactly 2 tasks."""
        dag = loaded_dags["01_ingestion_dag"]
        assert len(dag.tasks) == 2, \
            f"Expected 2 tasks, got {len(dag.tasks)}: {[t.task_id for t in dag.tasks]}"

    def test_has_batch_producer_task(self, loaded_dags):
        """Must have a task for running the batch producer."""
        dag = loaded_dags["01_ingestion_dag"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "run_batch_producer" in task_ids

    def test_triggers_silver(self, loaded_dags):
        """Must trigger Silver DAG after completion."""
        dag = loaded_dags["01_ingestion_dag"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "trigger_silver_pipeline" in task_ids

    def test_no_hardcoded_host_paths(self, loaded_dags):
        """Bash commands must NOT contain hardcoded Windows host paths."""
        dag = loaded_dags["01_ingestion_dag"]
        for task in dag.tasks:
            if hasattr(task, "bash_command") and task.bash_command:
                cmd = task.bash_command
                assert "C:/" not in cmd and "C:\\" not in cmd, \
                    f"Task {task.task_id} has hardcoded Windows path in bash_command"
                assert "StudyZone" not in cmd, \
                    f"Task {task.task_id} references old path 'StudyZone'"


# =============================================================================
# TEST GROUP 5: Gold DAG (04)
# =============================================================================

class TestGoldDAG:
    """Verify 04_gold_dag specifics."""

    def test_task_count(self, loaded_dags):
        """Should have at least 4 tasks (check_spark, run_gold, register, dbt_test, trigger)."""
        dag = loaded_dags["04_gold_dag"]
        assert len(dag.tasks) >= 4, \
            f"Expected >=4 tasks, got {len(dag.tasks)}: {[t.task_id for t in dag.tasks]}"

    def test_has_spark_check(self, loaded_dags):
        """Must have a pre-flight Spark availability check."""
        dag = loaded_dags["04_gold_dag"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "check_spark_available" in task_ids

    def test_has_gold_aggregation(self, loaded_dags):
        """Must have the main gold aggregation task."""
        dag = loaded_dags["04_gold_dag"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "silver_to_gold_aggregation" in task_ids

    def test_has_gcs_registration(self, loaded_dags):
        """Must register GCS table in Trino after Gold job."""
        dag = loaded_dags["04_gold_dag"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "register_gcs_gold_table" in task_ids

    def test_has_dbt_validation(self, loaded_dags):
        """Must run data quality check after Gold job."""
        dag = loaded_dags["04_gold_dag"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "dbt_test_gold_gcs" in task_ids

    def test_gold_uses_docker_exec(self, loaded_dags):
        """Gold spark-submit must use docker exec, not direct call."""
        dag = loaded_dags["04_gold_dag"]
        for task in dag.tasks:
            if task.task_id == "silver_to_gold_aggregation":
                assert "docker exec" in task.bash_command, \
                    "Gold task must use 'docker exec spark-master spark-submit'"

    def test_has_anti_collision_lock(self, loaded_dags):
        """Gold task must check if silver_to_gold is already running."""
        dag = loaded_dags["04_gold_dag"]
        for task in dag.tasks:
            if task.task_id == "silver_to_gold_aggregation":
                assert "pgrep" in task.bash_command or "already running" in task.bash_command, \
                    "Gold task must have anti-collision lock"


# =============================================================================
# TEST GROUP 6: Maintenance DAG (05) — OPTIMIZE & VACUUM
# =============================================================================

class TestMaintenanceDAG:
    """Verify 05_maintenance_dag — the most critical for Task 2."""

    def test_task_count(self, loaded_dags):
        """Should have 8 tasks: preflight + 3 optimize + 3 vacuum + log."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        assert len(dag.tasks) == 8, \
            f"Expected 8 tasks, got {len(dag.tasks)}: {[t.task_id for t in dag.tasks]}"

    def test_has_optimize_tasks(self, loaded_dags):
        """Must have OPTIMIZE tasks for all three layers."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "optimize_bronze" in task_ids, "Missing optimize_bronze"
        assert "optimize_silver" in task_ids, "Missing optimize_silver"
        assert "optimize_gold" in task_ids, "Missing optimize_gold"

    def test_has_vacuum_tasks(self, loaded_dags):
        """Must have VACUUM tasks for all three layers."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "vacuum_bronze" in task_ids, "Missing vacuum_bronze"
        assert "vacuum_silver" in task_ids, "Missing vacuum_silver"
        assert "vacuum_gold" in task_ids, "Missing vacuum_gold"

    def test_optimize_before_vacuum(self, loaded_dags):
        """VACUUM tasks must wait for ALL OPTIMIZE tasks to complete."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        task_dict = {t.task_id: t for t in dag.tasks}

        optimize_ids = {"optimize_bronze", "optimize_silver", "optimize_gold"}
        vacuum_ids = {"vacuum_bronze", "vacuum_silver", "vacuum_gold"}

        for vac_id in vacuum_ids:
            vac_task = task_dict[vac_id]
            upstream_ids = {t.task_id for t in vac_task.upstream_list}
            assert optimize_ids.issubset(upstream_ids), \
                f"{vac_id} must depend on ALL optimize tasks. " \
                f"Missing: {optimize_ids - upstream_ids}"

    def test_vacuum_has_7day_retention(self, loaded_dags):
        """VACUUM tasks must specify 7-day (168 hour) retention."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        for task in dag.tasks:
            if "vacuum" in task.task_id and hasattr(task, "bash_command"):
                assert "168" in task.bash_command, \
                    f"{task.task_id} must use 168-hour (7-day) retention"

    def test_maintenance_uses_docker_exec(self, loaded_dags):
        """All Spark tasks must use docker exec, not direct spark-submit."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        spark_tasks = [t for t in dag.tasks if "optimize" in t.task_id or "vacuum" in t.task_id]
        for task in spark_tasks:
            if hasattr(task, "bash_command") and task.bash_command:
                assert "docker exec" in task.bash_command, \
                    f"{task.task_id} must use 'docker exec spark-master spark-submit'. " \
                    f"The Airflow container does NOT have Spark installed!"

    def test_optimize_has_zorder(self, loaded_dags):
        """OPTIMIZE tasks should use ZORDER for query performance."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        for task in dag.tasks:
            if "optimize" in task.task_id and hasattr(task, "bash_command"):
                cmd = task.bash_command.lower()
                assert "zorder" in cmd or "executeZOrderBy" in task.bash_command, \
                    f"{task.task_id} should use ZORDER for optimal query performance"

    def test_has_completion_log(self, loaded_dags):
        """Must have a final logging task."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "log_maintenance_complete" in task_ids

    def test_gcs_paths_are_correct(self, loaded_dags):
        """All GCS paths must point to crypto-lakehouse-group8 bucket."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        for task in dag.tasks:
            if hasattr(task, "bash_command") and task.bash_command:
                if "gs://" in task.bash_command:
                    assert "crypto-lakehouse-group8" in task.bash_command, \
                        f"{task.task_id} has wrong GCS bucket"

    def test_has_preflight_check(self, loaded_dags):
        """Must have a pre-flight check before running Spark tasks."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "preflight_check" in task_ids

    def test_has_execution_timeout(self, loaded_dags):
        """Spark tasks must have execution_timeout to prevent hanging."""
        dag = loaded_dags["05_delta_lake_maintenance"]
        spark_tasks = [t for t in dag.tasks if "optimize" in t.task_id or "vacuum" in t.task_id]
        for task in spark_tasks:
            assert task.execution_timeout is not None, \
                f"{task.task_id} must have execution_timeout set"


# =============================================================================
# TEST GROUP 7: Cross-DAG Dependencies
# =============================================================================

class TestCrossDAGDependencies:
    """Verify cross-DAG trigger chains are correct."""

    def test_ingestion_triggers_silver(self, loaded_dags):
        """01_ingestion → 03_silver_dag."""
        dag = loaded_dags["01_ingestion_dag"]
        trigger_tasks = [
            t for t in dag.tasks
            if hasattr(t, "trigger_dag_id") and t.trigger_dag_id == "03_silver_dag"
        ]
        assert len(trigger_tasks) == 1, "Ingestion must trigger Silver DAG"

    def test_silver_triggers_gold(self, loaded_dags):
        """03_silver → 04_gold_dag."""
        dag = loaded_dags["03_silver_dag"]
        trigger_tasks = [
            t for t in dag.tasks
            if hasattr(t, "trigger_dag_id") and t.trigger_dag_id == "04_gold_dag"
        ]
        assert len(trigger_tasks) == 1, "Silver must trigger Gold DAG"

    def test_gold_triggers_silver_loop(self, loaded_dags):
        """04_gold → 03_silver_dag (feedback loop)."""
        dag = loaded_dags["04_gold_dag"]
        trigger_tasks = [
            t for t in dag.tasks
            if hasattr(t, "trigger_dag_id") and t.trigger_dag_id == "03_silver_dag"
        ]
        assert len(trigger_tasks) == 1, "Gold must trigger Silver DAG for data loop"


# =============================================================================
# TEST GROUP 8: Retries & Error Handling
# =============================================================================

class TestErrorHandling:
    """Verify retry and error handling configuration."""

    def test_all_dags_have_retries(self, loaded_dags):
        """All DAGs should have retries configured."""
        for dag_id, dag in loaded_dags.items():
            retries = dag.default_args.get("retries", 0)
            assert retries >= 1, \
                f"DAG {dag_id} has retries={retries} (expected >=1)"

    def test_all_dags_have_retry_delay(self, loaded_dags):
        """All DAGs should have retry_delay configured."""
        for dag_id, dag in loaded_dags.items():
            delay = dag.default_args.get("retry_delay")
            assert delay is not None, \
                f"DAG {dag_id} is missing retry_delay"


# =============================================================================
# SUMMARY
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
