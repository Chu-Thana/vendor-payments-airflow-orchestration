from pathlib import Path

from airflow.models import DagBag


# Resolve project root directory.
# This file is usually located in the tests/ folder, so parents[1]
# points back to the project root.
ROOT_DIR = Path(__file__).resolve().parents[1]

# Airflow DAG files should be stored in the dags/ directory.
DAGS_DIR = ROOT_DIR / "dags"


def test_dag_folder_exists():
    """
    Check that the dags/ folder exists.

    This prevents CI from passing if the Airflow DAG folder is missing
    or the project structure is incorrect.
    """
    assert DAGS_DIR.exists(), f"DAG folder not found: {DAGS_DIR}"


def test_airflow_dags_import_without_errors():
    """
    Load all DAG files using Airflow DagBag and check for import errors.

    This test catches syntax errors, missing imports, broken dependencies,
    or invalid DAG definitions before the DAG is deployed to Airflow.
    """
    dag_bag = DagBag(
        dag_folder=str(DAGS_DIR),
        include_examples=False,
    )

    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"
    assert len(dag_bag.dags) > 0, "No DAGs were loaded"


def test_vendor_payments_dag_has_expected_tasks():
    """
    Check that the vendor payments DAG is loaded and contains all expected tasks.

    This verifies the DAG structure at the task level. If a task is renamed,
    removed, or not created correctly, this test will fail.
    """
    dag_bag = DagBag(
        dag_folder=str(DAGS_DIR),
        include_examples=False,
    )

    dag = dag_bag.dags.get("vendor_payments_etl_orchestration")

    assert dag is not None, "vendor_payments_etl_orchestration DAG was not loaded"

    # Expected task IDs in the orchestration workflow.
    expected_tasks = {
        "start",
        "check_project1_source",
        "clean_previous_outputs",
        "run_vendor_payments_pipeline",
        "check_silver_output",
        "check_gold_outputs",
        "end",
    }

    # The DAG should contain exactly these tasks:
    # no missing tasks and no unexpected extra tasks.
    assert set(dag.task_ids) == expected_tasks


def test_vendor_payments_dag_task_dependencies():
    """
    Check that task dependencies are wired in the correct order.

    This validates the Airflow workflow sequence:

    start
    -> check_project1_source
    -> clean_previous_outputs
    -> run_vendor_payments_pipeline
    -> check_silver_output
    -> check_gold_outputs
    -> end
    """
    dag_bag = DagBag(
        dag_folder=str(DAGS_DIR),
        include_examples=False,
    )

    dag = dag_bag.dags.get("vendor_payments_etl_orchestration")

    assert dag is not None, "vendor_payments_etl_orchestration DAG was not loaded"

    # Each assertion checks the downstream task of one task.
    # This ensures the pipeline runs in the intended order.
    assert dag.get_task("start").downstream_task_ids == {"check_project1_source"}

    assert dag.get_task("check_project1_source").downstream_task_ids == {
        "clean_previous_outputs"
    }

    assert dag.get_task("clean_previous_outputs").downstream_task_ids == {
        "run_vendor_payments_pipeline"
    }

    assert dag.get_task("run_vendor_payments_pipeline").downstream_task_ids == {
        "check_silver_output"
    }

    assert dag.get_task("check_silver_output").downstream_task_ids == {
        "check_gold_outputs"
    }

    assert dag.get_task("check_gold_outputs").downstream_task_ids == {"end"}

    # The end task should be the final node in the DAG.
    # It should not trigger any downstream tasks.
    assert dag.get_task("end").downstream_task_ids == set()