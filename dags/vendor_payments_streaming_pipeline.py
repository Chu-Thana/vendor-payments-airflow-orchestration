from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG

from scripts.notify import task_fail_alert, notify_success
from scripts.staging.extract_vendor_payments_staging import (
    extract_vendor_payments_staging,
)
from scripts.staging.transform_vendor_payments_staging import (
    transform_vendor_payments_staging,
)
from scripts.staging.load_vendor_payments_summary import (
    load_vendor_payments_summary,
)

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


STREAMING_PIPELINE_ROOT = Path(
    "/opt/airflow/vendor_payments_streaming"
)

STREAMING_STAGING_DIR = (
    STREAMING_PIPELINE_ROOT
    / "output"
    / "staging"
)


default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": task_fail_alert,
}


def discover_completed_streaming_window() -> dict:
    """Find one completed streaming window ready for downstream processing."""
    if not STREAMING_STAGING_DIR.exists():
        raise FileNotFoundError(
            "Streaming staging directory not found: "
            f"{STREAMING_STAGING_DIR}"
        )

    completed_windows = []

    for window_dir in sorted(
        STREAMING_STAGING_DIR.glob("stream_window_*")
    ):
        if not window_dir.is_dir():
            continue

        staging_file = window_dir / "events.jsonl"
        success_marker = window_dir / "_SUCCESS"
        processed_marker = window_dir / "_PROCESSED"

        if (
                staging_file.exists()
                and success_marker.exists()
                and not processed_marker.exists()
        ):
            completed_windows.append(window_dir)

    if not completed_windows:
        raise FileNotFoundError(
            "No completed streaming window found."
        )

    selected_window = completed_windows[0]
    staging_file = selected_window / "events.jsonl"
    success_marker = selected_window / "_SUCCESS"

    if staging_file.stat().st_size == 0:
        raise ValueError(
            "Completed streaming window contains "
            f"an empty staging file: {staging_file}"
        )

    return {
        "window_id": selected_window.name,
        "staging_file": str(staging_file),
        "success_marker": str(success_marker),
        "staging_file_size_bytes": staging_file.stat().st_size,
        "status": "ready",
    }


def mark_streaming_window_processed(
    window_id: str,
) -> str:
    """Mark a completed streaming window as processed by Airflow."""
    window_dir = STREAMING_STAGING_DIR / window_id

    if not window_dir.exists():
        raise FileNotFoundError(
            f"Streaming window directory not found: {window_dir}"
        )

    success_marker = window_dir / "_SUCCESS"

    if not success_marker.exists():
        raise FileNotFoundError(
            "Streaming window success marker not found: "
            f"{success_marker}"
        )

    processed_marker = window_dir / "_PROCESSED"
    processed_marker.touch()

    return str(processed_marker)


with DAG(
    dag_id="vendor_payments_streaming_pipeline",
    default_args=default_args,
    description=(
            "Process completed Vendor Payments streaming windows "
            "through extract, transform, and load tasks"
    ),
    start_date=datetime(2026, 4, 14),
    schedule=None,
    catchup=False,
    tags=[
        "vendor-payments",
        "streaming",
        "window",
        "pipeline",
    ],
    on_success_callback=notify_success,
) as dag:
    discover_window_task = PythonOperator(
        task_id="discover_completed_streaming_window",
        python_callable=discover_completed_streaming_window,
    )

    extract_task = PythonOperator(
        task_id="extract_vendor_payments_staging",
        python_callable=extract_vendor_payments_staging,
        op_kwargs={
            "staging_file": (
                "{{ ti.xcom_pull("
                "task_ids='discover_completed_streaming_window'"
                ")['staging_file'] }}"
            ),
            "window_id": (
                "{{ ti.xcom_pull("
                "task_ids='discover_completed_streaming_window'"
                ")['window_id'] }}"
            ),
        },
    )

    transform_task = PythonOperator(
        task_id="transform_vendor_payments_staging",
        python_callable=transform_vendor_payments_staging,
        op_kwargs={
            "input_file": (
                "{{ ti.xcom_pull("
                "task_ids='extract_vendor_payments_staging'"
                ") }}"
            ),
            "window_id": (
                "{{ ti.xcom_pull("
                "task_ids='discover_completed_streaming_window'"
                ")['window_id'] }}"
            ),
        },
    )

    load_task = PythonOperator(
        task_id="load_vendor_payments_summary",
        python_callable=load_vendor_payments_summary,
        op_kwargs={
            "input_file": (
                "{{ ti.xcom_pull("
                "task_ids='transform_vendor_payments_staging'"
                ") }}"
            ),
            "window_id": (
                "{{ ti.xcom_pull("
                "task_ids='discover_completed_streaming_window'"
                ")['window_id'] }}"
            ),
        },
    )

    mark_processed_task = PythonOperator(
        task_id="mark_streaming_window_processed",
        python_callable=mark_streaming_window_processed,
        op_kwargs={
            "window_id": (
                "{{ ti.xcom_pull("
                "task_ids='discover_completed_streaming_window'"
                ")['window_id'] }}"
            ),
        },
    )

    (
            discover_window_task
            >> extract_task
            >> transform_task
            >> load_task
            >> mark_processed_task
    )