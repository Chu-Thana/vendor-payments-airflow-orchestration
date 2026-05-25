from pathlib import Path

from airflow.models import DagBag


ROOT_DIR = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT_DIR / "dags"


def test_dag_folder_exists():
    assert DAGS_DIR.exists(), f"DAG folder not found: {DAGS_DIR}"


def test_airflow_dags_import_without_errors():
    dag_bag = DagBag(
        dag_folder=str(DAGS_DIR),
        include_examples=False,
    )

    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"
    assert len(dag_bag.dags) > 0, "No DAGs were loaded"