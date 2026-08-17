from pathlib import Path

from airflow.models import DagBag


ROOT_DIR = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT_DIR / "dags"

DAG_ID = "vendor_payments_data_platform_orchestration"


def load_dag_bag() -> DagBag:
    return DagBag(
        dag_folder=str(DAGS_DIR),
    )


def test_dag_folder_exists() -> None:
    assert DAGS_DIR.exists(), f"DAG folder not found: {DAGS_DIR}"


def test_airflow_dags_import_without_errors() -> None:
    dag_bag = load_dag_bag()

    assert not dag_bag.import_errors, (
        f"DAG import errors: {dag_bag.import_errors}"
    )
    assert len(dag_bag.dags) > 0, "No DAGs were loaded"


def test_vendor_payments_dag_is_loaded() -> None:
    dag_bag = load_dag_bag()
    dag = dag_bag.dags.get(DAG_ID)

    assert dag is not None, f"{DAG_ID} DAG was not loaded"


def test_vendor_payments_dag_has_expected_tasks() -> None:
    dag_bag = load_dag_bag()
    dag = dag_bag.dags.get(DAG_ID)

    assert dag is not None, f"{DAG_ID} DAG was not loaded"

    expected_tasks = {
        "start",
        "check_batch_etl_ready",
        "run_batch_etl_pipeline",
        "validate_silver_output",
        "validate_gold_outputs",
        "upload_batch_gold_to_s3",
        "check_streaming_staging_ready",
        "run_downstream_deduplication_check",
        "convert_streaming_jsonl_to_csv",
        "upload_streaming_curated_to_s3",
        "upload_streaming_reports_to_s3",
        "check_cloud_platform_ready",
        "redshift_create_schemas",
        "redshift_create_batch_landing_tables",
        "redshift_copy_batch_gold_from_s3",
        "redshift_create_batch_analytics_views",
        "redshift_validate_batch_analytics",
        "redshift_create_streaming_landing_table",
        "redshift_copy_streaming_curated_from_s3",
        "redshift_create_streaming_analytics_views",
        "redshift_validate_streaming_analytics",
        "generate_redshift_execution_summary",
        "validate_redshift_execution_summary",
        "generate_orchestration_summary",
        "end",
    }

    assert set(dag.task_ids) == expected_tasks
    assert len(dag.task_ids) == 25


def test_vendor_payments_dag_task_dependencies() -> None:
    dag_bag = load_dag_bag()
    dag = dag_bag.dags.get(DAG_ID)

    assert dag is not None, f"{DAG_ID} DAG was not loaded"

    expected_dependencies = {
        "start": {"check_batch_etl_ready"},
        "check_batch_etl_ready": {"run_batch_etl_pipeline"},
        "run_batch_etl_pipeline": {"validate_silver_output"},
        "validate_silver_output": {"validate_gold_outputs"},
        "validate_gold_outputs": {"upload_batch_gold_to_s3"},
        "upload_batch_gold_to_s3": {"check_streaming_staging_ready"},
        "check_streaming_staging_ready": {
            "run_downstream_deduplication_check"
        },
        "run_downstream_deduplication_check": {
            "convert_streaming_jsonl_to_csv"
        },
        "convert_streaming_jsonl_to_csv": {
            "upload_streaming_curated_to_s3"
        },
        "upload_streaming_curated_to_s3": {
            "check_cloud_platform_ready"
        },
        "check_cloud_platform_ready": {"redshift_create_schemas"},

        "redshift_create_schemas": {
            "redshift_create_batch_landing_tables",
            "redshift_create_streaming_landing_table",
        },

        "redshift_create_batch_landing_tables": {
            "redshift_copy_batch_gold_from_s3"
        },
        "redshift_copy_batch_gold_from_s3": {
            "redshift_create_batch_analytics_views"
        },
        "redshift_create_batch_analytics_views": {
            "redshift_validate_batch_analytics"
        },
        "redshift_validate_batch_analytics": {
            "generate_redshift_execution_summary"
        },

        "redshift_create_streaming_landing_table": {
            "redshift_copy_streaming_curated_from_s3"
        },
        "redshift_copy_streaming_curated_from_s3": {
            "redshift_create_streaming_analytics_views"
        },
        "redshift_create_streaming_analytics_views": {
            "redshift_validate_streaming_analytics"
        },
        "redshift_validate_streaming_analytics": {
            "generate_redshift_execution_summary"
        },

        "generate_redshift_execution_summary": {
            "validate_redshift_execution_summary"
        },
        "validate_redshift_execution_summary": {
            "generate_orchestration_summary"
        },
        "generate_orchestration_summary": {
            "upload_streaming_reports_to_s3"
        },
        "upload_streaming_reports_to_s3": {"end"},
        "end": set(),
    }

    for task_id, downstream_task_ids in expected_dependencies.items():
        assert (
            dag.get_task(task_id).downstream_task_ids
            == downstream_task_ids
        )


def test_vendor_payments_dag_configuration() -> None:
    dag_bag = load_dag_bag()
    dag = dag_bag.dags.get(DAG_ID)

    assert dag is not None, f"{DAG_ID} DAG was not loaded"

    assert dag.timetable.__class__.__name__ == "NullTimetable"
    assert dag.catchup is False

    expected_tags = {
        "vendor-payments",
        "batch",
        "streaming",
        "redshift",
        "orchestration",
    }

    assert set(dag.tags) == expected_tags


def test_redshift_tasks_exist_in_dependency_order() -> None:
    dag_bag = load_dag_bag()
    dag = dag_bag.dags.get(DAG_ID)

    assert dag is not None, f"{DAG_ID} DAG was not loaded"

    check_cloud_platform_task = dag.get_task(
        "check_cloud_platform_ready"
    )
    create_schemas_task = dag.get_task(
        "redshift_create_schemas"
    )
    batch_validation_task = dag.get_task(
        "redshift_validate_batch_analytics"
    )
    streaming_validation_task = dag.get_task(
        "redshift_validate_streaming_analytics"
    )
    generate_summary_task = dag.get_task(
        "generate_redshift_execution_summary"
    )
    validate_summary_task = dag.get_task(
        "validate_redshift_execution_summary"
    )
    orchestration_summary_task = dag.get_task(
        "generate_orchestration_summary"
    )

    assert check_cloud_platform_task.downstream_task_ids == {
        "redshift_create_schemas"
    }

    assert create_schemas_task.downstream_task_ids == {
        "redshift_create_batch_landing_tables",
        "redshift_create_streaming_landing_table",
    }

    assert batch_validation_task.downstream_task_ids == {
        "generate_redshift_execution_summary"
    }

    assert streaming_validation_task.downstream_task_ids == {
        "generate_redshift_execution_summary"
    }

    assert generate_summary_task.upstream_task_ids == {
        "redshift_validate_batch_analytics",
        "redshift_validate_streaming_analytics",
    }

    assert generate_summary_task.downstream_task_ids == {
        "validate_redshift_execution_summary"
    }

    assert validate_summary_task.downstream_task_ids == {
        "generate_orchestration_summary"
    }

    assert orchestration_summary_task.upstream_task_ids == {
        "validate_redshift_execution_summary"
    }

