from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dags import vendor_payments_etl_orchestration as orchestration


def build_task_instance(
    task_id: str,
    state: str,
    try_number: int = 1,
    duration: float | None = 1.0,
) -> SimpleNamespace:
    started_at = datetime(
        2026,
        6,
        22,
        10,
        0,
        tzinfo=timezone.utc,
    )
    completed_at = datetime(
        2026,
        6,
        22,
        10,
        1,
        tzinfo=timezone.utc,
    )

    return SimpleNamespace(
        task_id=task_id,
        state=state,
        try_number=try_number,
        start_date=started_at,
        end_date=completed_at,
        duration=duration,
    )


def build_dag_run(
    task_instances: list[SimpleNamespace],
) -> SimpleNamespace:
    started_at = datetime(
        2026,
        6,
        22,
        10,
        0,
        tzinfo=timezone.utc,
    )
    completed_at = datetime(
        2026,
        6,
        22,
        10,
        16,
        36,
        tzinfo=timezone.utc,
    )

    return SimpleNamespace(
        dag_id="vendor_payments_data_platform_orchestration",
        run_id="manual__2026-06-22T10:00:00+00:00",
        run_type="manual",
        logical_date=started_at,
        start_date=started_at,
        end_date=completed_at,
        get_task_instances=lambda: task_instances,
    )


def test_build_airflow_execution_metadata_success() -> None:
    task_instances = [
        build_task_instance("start", "success"),
        build_task_instance("run_pipeline", "success"),
        build_task_instance("end", "success"),
    ]
    dag_run = build_dag_run(task_instances)

    result = orchestration.build_airflow_execution_metadata(
        context={"dag_run": dag_run},
        final_status="success",
    )

    execution = result["execution"]
    task_metrics = result["task_metrics"]

    assert execution["dag_id"] == (
        "vendor_payments_data_platform_orchestration"
    )
    assert execution["run_type"] == "manual"
    assert execution["runtime_seconds"] == 996.0
    assert execution["final_status"] == "success"

    assert task_metrics["total_task_count"] == 3
    assert task_metrics["successful_task_count"] == 3
    assert task_metrics["failed_task_count"] == 0
    assert task_metrics["retry_attempt_count"] == 0
    assert task_metrics["state_counts"] == {
        "success": 3,
    }


def test_build_airflow_execution_metadata_counts_states() -> None:
    task_instances = [
        build_task_instance("task_success", "success"),
        build_task_instance("task_failed", "failed"),
        build_task_instance("task_skipped", "skipped"),
        build_task_instance(
            "task_upstream_failed",
            "upstream_failed",
        ),
        build_task_instance(
            "task_up_for_retry",
            "up_for_retry",
        ),
    ]
    dag_run = build_dag_run(task_instances)

    result = orchestration.build_airflow_execution_metadata(
        context={"dag_run": dag_run},
        final_status="failed",
    )

    task_metrics = result["task_metrics"]

    assert task_metrics["total_task_count"] == 5
    assert task_metrics["successful_task_count"] == 1
    assert task_metrics["failed_task_count"] == 1
    assert task_metrics["skipped_task_count"] == 1
    assert task_metrics["upstream_failed_task_count"] == 1
    assert task_metrics["up_for_retry_task_count"] == 1


def test_build_airflow_execution_metadata_counts_retries() -> None:
    task_instances = [
        build_task_instance(
            "task_without_retry",
            "success",
            try_number=1,
        ),
        build_task_instance(
            "task_with_retry",
            "success",
            try_number=3,
        ),
    ]
    dag_run = build_dag_run(task_instances)

    result = orchestration.build_airflow_execution_metadata(
        context={"dag_run": dag_run},
        final_status="success",
    )

    task_metrics = result["task_metrics"]
    task_details = task_metrics["task_execution_details"]

    assert task_metrics["retry_attempt_count"] == 2

    retried_task = next(
        item
        for item in task_details
        if item["task_id"] == "task_with_retry"
    )

    assert retried_task["try_number"] == 3
    assert retried_task["retry_attempts"] == 2


def test_build_airflow_execution_metadata_requires_dag_run() -> None:
    with pytest.raises(
        ValueError,
        match="Airflow DAG run context is unavailable",
    ):
        orchestration.build_airflow_execution_metadata(
            context={},
            final_status="failed",
        )


def test_finalize_orchestration_summary_writes_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    summary_file = (
        tmp_path
        / "airflow_orchestration_summary.json"
    )

    summary_file.write_text(
        '{"project": "Vendor Payments Airflow Orchestration"}',
        encoding="utf-8",
    )

    task_instances = [
        build_task_instance("start", "success"),
        build_task_instance("end", "success"),
    ]
    dag_run = build_dag_run(task_instances)

    monkeypatch.setattr(
        orchestration,
        "ORCHESTRATION_SUMMARY",
        summary_file,
    )

    orchestration.finalize_orchestration_summary(
        context={"dag_run": dag_run},
        final_status="success",
    )

    content = summary_file.read_text(
        encoding="utf-8",
    )

    assert '"final_status": "success"' in content
    assert '"total_task_count": 2' in content
    assert '"successful_task_count": 2' in content
    assert '"status": "PASS"' in content