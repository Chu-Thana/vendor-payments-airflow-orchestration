from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session
from sqlalchemy import select

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator


BATCH_PIPELINE_DAG_ID = ("vendor_payments_batch_pipeline")
STREAMING_PIPELINE_DAG_ID = ("vendor_payments_streaming_pipeline")

STREAMING_PIPELINE_ROOT = Path("/opt/airflow/vendor_payments_streaming")
CLOUD_PLATFORM_ROOT = Path("/opt/airflow/vendor_payments_cloud_platform")
ORCHESTRATION_OUTPUT_ROOT = Path("/opt/airflow/output")

REDSHIFT_SUMMARY_SCRIPT = CLOUD_PLATFORM_ROOT / "scripts/warehouse/generate_redshift_summary.py"
REDSHIFT_EXECUTION_SUMMARY =  CLOUD_PLATFORM_ROOT / "output/reports/redshift_execution_summary.json"
ORCHESTRATION_SUMMARY =  ORCHESTRATION_OUTPUT_ROOT / "reports/airflow_orchestration_summary.json"


@provide_session
def check_pipeline_status(
    dag_id: str,
    session=None,
) -> dict:
    """Return the latest Airflow run status for one pipeline."""

    latest_run = session.scalar(
        select(DagRun)
        .where(DagRun.dag_id == dag_id)
        .order_by(DagRun.logical_date.desc())
        .limit(1)
    )

    if latest_run is None:
        raise FileNotFoundError(
            f"No Airflow DAG run found for: {dag_id}"
        )

    run_state = enum_value(
        latest_run.state
    )

    if run_state != "success":
        raise ValueError(
            f"Latest DAG run is not successful: "
            f"dag_id={dag_id} state={run_state}"
        )

    return {
        "dag_id": dag_id,
        "run_id": latest_run.run_id,
        "state": run_state,
        "logical_date": serialize_datetime(
            latest_run.logical_date
        ),
        "started_at": serialize_datetime(
            latest_run.start_date
        ),
        "completed_at": serialize_datetime(
            latest_run.end_date
        ),
        "status": "ready",
    }


def check_batch_pipeline_status() -> dict:
    return check_pipeline_status(
        BATCH_PIPELINE_DAG_ID
    )


def check_streaming_pipeline_status() -> dict:
    return check_pipeline_status(
        STREAMING_PIPELINE_DAG_ID
    )


def check_cloud_platform_ready() -> dict:
    if not CLOUD_PLATFORM_ROOT.exists():
        raise FileNotFoundError(
            f"Cloud platform root not found: {CLOUD_PLATFORM_ROOT}"
        )

    if not REDSHIFT_SUMMARY_SCRIPT.exists():
        raise FileNotFoundError(
            "Redshift summary script not found: "
            f"{REDSHIFT_SUMMARY_SCRIPT}"
        )

    return {
        "cloud_platform_root": str(CLOUD_PLATFORM_ROOT),
        "redshift_summary_script": str(
            REDSHIFT_SUMMARY_SCRIPT
        ),
        "cloud_platform_readiness_status": "passed",
    }


def serialize_datetime(value: Any) -> str | None:
    if value is None:
        return None

    return value.isoformat()


def enum_value(value: Any) -> str | None:
    if value is None:
        return None

    return str(getattr(value, "value", value))


def build_airflow_execution_metadata(
    context: dict[str, Any],
    final_status: str,
) -> dict[str, Any]:
    dag_run = context.get("dag_run")

    if dag_run is None:
        raise ValueError(
            "Airflow DAG run context is unavailable."
        )

    task_instances = dag_run.get_task_instances()

    state_counts: dict[str, int] = {}
    retry_attempt_count = 0
    task_execution_details: list[dict[str, Any]] = []

    for task_instance in task_instances:
        task_state = enum_value(
            task_instance.state
        ) or "none"

        state_counts[task_state] = (
            state_counts.get(task_state, 0) + 1
        )

        try_number = int(
            task_instance.try_number or 0
        )
        retries_for_task = max(
            try_number - 1,
            0,
        )
        retry_attempt_count += retries_for_task

        task_execution_details.append(
            {
                "task_id": task_instance.task_id,
                "state": task_state,
                "try_number": try_number,
                "retry_attempts": retries_for_task,
                "started_at": serialize_datetime(
                    task_instance.start_date
                ),
                "completed_at": serialize_datetime(
                    task_instance.end_date
                ),
                "duration_seconds": task_instance.duration,
            }
        )

    started_at = dag_run.start_date
    completed_at = (
        dag_run.end_date
        or datetime.now(timezone.utc)
    )

    runtime_seconds = None

    if started_at is not None:
        runtime_seconds = round(
            (
                completed_at
                - started_at
            ).total_seconds(),
            2,
        )

    return {
        "execution": {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "run_type": enum_value(
                dag_run.run_type
            ),
            "logical_date": serialize_datetime(
                dag_run.logical_date
            ),
            "started_at": serialize_datetime(
                started_at
            ),
            "completed_at": serialize_datetime(
                completed_at
            ),
            "runtime_seconds": runtime_seconds,
            "final_status": final_status,
        },
        "task_metrics": {
            "total_task_count": len(task_instances),
            "successful_task_count": state_counts.get(
                "success",
                0,
            ),
            "failed_task_count": state_counts.get(
                "failed",
                0,
            ),
            "skipped_task_count": state_counts.get(
                "skipped",
                0,
            ),
            "upstream_failed_task_count": state_counts.get(
                "upstream_failed",
                0,
            ),
            "up_for_retry_task_count": state_counts.get(
                "up_for_retry",
                0,
            ),
            "retry_attempt_count": retry_attempt_count,
            "state_counts": state_counts,
            "task_execution_details": (
                task_execution_details
            ),
        },
    }


def finalize_orchestration_summary(
    context: dict[str, Any],
    final_status: str,
) -> None:
    ORCHESTRATION_SUMMARY.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary: dict[str, Any] = {}

    if ORCHESTRATION_SUMMARY.exists():
        with ORCHESTRATION_SUMMARY.open(
            "r",
            encoding="utf-8",
        ) as file:
            summary = json.load(file)

    execution_metadata = (
        build_airflow_execution_metadata(
            context=context,
            final_status=final_status,
        )
    )

    summary.setdefault(
        "project",
        "Vendor Payments Airflow Orchestration",
    )
    summary.setdefault(
        "pipeline_version",
        "1.0.0",
    )

    summary["finalized_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    summary["execution"] = execution_metadata[
        "execution"
    ]
    summary["task_metrics"] = execution_metadata[
        "task_metrics"
    ]
    summary["orchestration_status"] = final_status
    summary["validation"] = {
        "status": (
            "PASS"
            if final_status == "success"
            else "FAIL"
        )
    }

    temporary_file = ORCHESTRATION_SUMMARY.with_suffix(
        ".json.tmp"
    )

    with temporary_file.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=2,
        )

    temporary_file.replace(
        ORCHESTRATION_SUMMARY
    )


def finalize_successful_dag(
    context: dict[str, Any],
) -> None:
    finalize_orchestration_summary(
        context=context,
        final_status="success",
    )


def finalize_failed_dag(
    context: dict[str, Any],
) -> None:
    finalize_orchestration_summary(
        context=context,
        final_status="failed",
    )


def generate_redshift_execution_summary() -> dict:
    result = subprocess.run(
        [
            "python",
            str(REDSHIFT_SUMMARY_SCRIPT),
        ],
        cwd=str(CLOUD_PLATFORM_ROOT),
        env={
            **os.environ,
            "PYTHONPATH": str(CLOUD_PLATFORM_ROOT),
        },
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Cloud platform redshift summary generation failed.\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    if not REDSHIFT_EXECUTION_SUMMARY.exists():
        raise FileNotFoundError(
            "Redshift execution summary was not generated: "
            f"{REDSHIFT_EXECUTION_SUMMARY}"
        )

    return {
        "summary_file": str(REDSHIFT_EXECUTION_SUMMARY),
        "generation_status": "passed",
        "stdout": result.stdout.strip(),
    }


def validate_redshift_execution_summary() -> dict:
    if not REDSHIFT_EXECUTION_SUMMARY.exists():
        raise FileNotFoundError(
            "Redshift execution summary not found: "
            f"{REDSHIFT_EXECUTION_SUMMARY}"
        )

    with REDSHIFT_EXECUTION_SUMMARY.open(
        "r",
        encoding="utf-8",
    ) as file:
        summary = json.load(file)

    execution = summary.get("execution", {})
    batch = summary.get("batch", {})
    streaming = summary.get("streaming", {})
    validation = summary.get("validation", {})

    landing_metrics = streaming.get(
        "landing_metrics",
        {},
    )
    analytics_metrics = streaming.get(
        "analytics_metrics",
        {},
    )

    total_rows = int(
        landing_metrics.get("total_rows", 0)
    )
    distinct_event_ids = int(
        landing_metrics.get("distinct_event_ids", 0)
    )
    duplicate_event_ids = int(
        landing_metrics.get("duplicate_event_ids", 0)
    )
    missing_event_ids = int(
        landing_metrics.get("missing_event_ids", 0)
    )
    analytics_total_events = int(
        analytics_metrics.get("total_events", 0)
    )

    validation_errors: list[str] = []

    if execution.get("status") != "PASS":
        validation_errors.append(
            "Redshift execution status is not PASS."
        )

    if batch.get("validation_status") != "PASS":
        validation_errors.append(
            "Batch Redshift validation status is not PASS."
        )

    if streaming.get("validation_status") != "PASS":
        validation_errors.append(
            "Streaming Redshift validation status is not PASS."
        )

    if validation.get("status") != "PASS":
        validation_errors.append(
            "Overall Redshift validation status is not PASS."
        )

    if int(batch.get("landing_table_count", 0)) != 5:
        validation_errors.append(
            "Expected 5 Batch landing tables."
        )

    if int(batch.get("analytics_view_count", 0)) != 5:
        validation_errors.append(
            "Expected 5 Batch analytics views."
        )

    if int(streaming.get("analytics_view_count", 0)) != 4:
        validation_errors.append(
            "Expected 4 Streaming analytics views."
        )

    if total_rows <= 0:
        validation_errors.append(
            "Streaming landing table contains no rows."
        )

    if distinct_event_ids != total_rows:
        validation_errors.append(
            "Distinct event IDs do not match total rows."
        )

    if duplicate_event_ids != 0:
        validation_errors.append(
            "Duplicate event IDs were detected."
        )

    if missing_event_ids != 0:
        validation_errors.append(
            "Missing event IDs were detected."
        )

    if analytics_total_events != total_rows:
        validation_errors.append(
            "Analytics total events do not match landing rows."
        )

    if validation_errors:
        raise ValueError(
            "Redshift metadata validation failed: "
            + " ".join(validation_errors)
        )

    return {
        "available": True,
        "summary_file": str(REDSHIFT_EXECUTION_SUMMARY),
        "execution_status": execution["status"],
        "runtime_seconds": execution["runtime_seconds"],
        "redshift": summary["redshift"],
        "batch": batch,
        "streaming": streaming,
        "validation_status": validation["status"],
    }


def generate_orchestration_summary(**context) -> None:
    task_instance = context["ti"]

    batch_pipeline_status = task_instance.xcom_pull(
        task_ids="check_batch_pipeline_status"
    )

    streaming_pipeline_status = task_instance.xcom_pull(
        task_ids="check_streaming_pipeline_status"
    )

    cloud_platform_readiness = task_instance.xcom_pull(
        task_ids="check_cloud_platform_ready"
    )

    redshift_generation = task_instance.xcom_pull(
        task_ids="generate_redshift_execution_summary"
    )

    redshift_validation = task_instance.xcom_pull(
        task_ids="validate_redshift_execution_summary"
    )

    ORCHESTRATION_SUMMARY.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary = {
        "project": "Vendor Payments Data Platform",
        "pipeline_version": "2.0.0",
        "dag_id": "vendor_payments_data_platform_orchestration",
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "batch_pipeline": batch_pipeline_status,
        "streaming_pipeline": streaming_pipeline_status,
        "platform_pipeline": {
            "cloud_platform_readiness": (
                cloud_platform_readiness
            ),
            "redshift_summary_generation": (
                redshift_generation
            ),
            "redshift_validation": (
                redshift_validation
            ),
        },
        "orchestration_status": "completed",
        "validation": {
            "status": "PASS",
        },
        "design_note": (
            "Batch and Streaming workloads run as independent "
            "Airflow lifecycles. This DAG validates the latest "
            "successful pipeline runs and platform-level metadata."
        ),
    }

    with ORCHESTRATION_SUMMARY.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=2,
        )


with DAG(
    dag_id="vendor_payments_data_platform_orchestration",
    description=(
        "Orchestrates Vendor Payments Batch ETL, streaming validation, "
        "downstream deduplication, and Amazon Redshift metadata validation."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=finalize_successful_dag,
    on_failure_callback=finalize_failed_dag,
    tags=[
        "vendor-payments",
        "batch",
        "streaming",
        "redshift",
        "orchestration",
    ],
) as dag:
    start = EmptyOperator(
        task_id="start"
    )

    check_cloud_platform_ready_task = PythonOperator(
        task_id="check_cloud_platform_ready",
        python_callable=check_cloud_platform_ready,
    )

    generate_redshift_execution_summary_task = PythonOperator(
        task_id="generate_redshift_execution_summary",
        python_callable=generate_redshift_execution_summary,
    )

    validate_redshift_execution_summary_task = PythonOperator(
        task_id="validate_redshift_execution_summary",
        python_callable=validate_redshift_execution_summary,
    )

    generate_orchestration_summary_task = PythonOperator(
        task_id="generate_orchestration_summary",
        python_callable=generate_orchestration_summary,
    )

    check_batch_pipeline_status_task = PythonOperator(
        task_id="check_batch_pipeline_status",
        python_callable=check_batch_pipeline_status,
    )

    check_streaming_pipeline_status_task = PythonOperator(
        task_id="check_streaming_pipeline_status",
        python_callable=check_streaming_pipeline_status,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> [
        check_batch_pipeline_status_task,
        check_streaming_pipeline_status_task,
    ]

    [
        check_batch_pipeline_status_task,
        check_streaming_pipeline_status_task,
    ] >> check_cloud_platform_ready_task

    (
            check_cloud_platform_ready_task
            >> generate_redshift_execution_summary_task
            >> validate_redshift_execution_summary_task
            >> generate_orchestration_summary_task
            >> end
    )