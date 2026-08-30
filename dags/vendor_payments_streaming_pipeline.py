from __future__ import annotations

import os
import subprocess
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

CLOUD_PLATFORM_ROOT = Path(
    "/opt/airflow/vendor_payments_cloud_platform"
)

STREAMING_CURATED_CONVERTER_SCRIPT = (
    CLOUD_PLATFORM_ROOT
    / "scripts"
    / "streaming"
    / "convert_streaming_jsonl_to_csv.py"
)

STREAMING_CURATED_UPLOAD_SCRIPT = (
    CLOUD_PLATFORM_ROOT
    / "scripts"
    / "streaming"
    / "upload_streaming_curated_to_s3.py"
)

STREAMING_LATEST_POINTER_SCRIPT = (
    CLOUD_PLATFORM_ROOT
    / "scripts"
    / "streaming"
    / "publish_latest_streaming_pointer.py"
)

STREAMING_CURATED_DIR = (
    CLOUD_PLATFORM_ROOT
    / "data"
    / "streaming"
    / "curated"
)

REDSHIFT_SQL_RUNNER = (
    CLOUD_PLATFORM_ROOT
    / "scripts"
    / "warehouse"
    / "run_redshift_sql.py"
)

REDSHIFT_CREATE_STREAMING_LANDING_TABLE_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "06_create_streaming_landing_table.sql"
)

REDSHIFT_COPY_STREAMING_CURATED_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "07_copy_streaming_curated_from_s3.sql"
)

REDSHIFT_CREATE_STREAMING_ANALYTICS_VIEWS_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "08_create_streaming_analytics_views.sql"
)

REDSHIFT_VALIDATE_STREAMING_ANALYTICS_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "09_validate_streaming_analytics.sql"
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


def convert_streaming_window_to_curated(
    staging_file: str,
    window_id: str,
) -> str:
    output_file = (
        STREAMING_CURATED_DIR
        / window_id
        / "vendor_payments_streaming_events.csv"
    )

    result = subprocess.run(
        [
            "python",
            str(STREAMING_CURATED_CONVERTER_SCRIPT),
            "--input-file",
            staging_file,
            "--output-file",
            str(output_file),
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
            "Streaming curated conversion failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return str(output_file)


def upload_streaming_window_curated(
    curated_file: str,
    window_id: str,
) -> str:
    result = subprocess.run(
        [
            "python",
            str(STREAMING_CURATED_UPLOAD_SCRIPT),
            "--input-file",
            curated_file,
            "--window-id",
            window_id,
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
            "Streaming curated upload failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    output_lines = [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    ]

    s3_uri_line = next(
        (
            line
            for line in reversed(output_lines)
            if line.startswith(
            "Streaming curated CSV uploaded:"
        )
        ),
        None,
    )

    if s3_uri_line is None:
        raise RuntimeError(
            "Streaming curated S3 URI was not returned."
        )

    return s3_uri_line.split(
        "Streaming curated CSV uploaded:",
        maxsplit=1,
    )[1].strip()


def run_redshift_sql_task(
    sql_file: Path,
    failure_message: str,
    status_key: str,
    extra_env: dict[str, str] | None = None,
) -> dict:
    environment = {
        **os.environ,
        "PYTHONPATH": str(CLOUD_PLATFORM_ROOT),
    }

    if extra_env:
        environment.update(extra_env)

    result = subprocess.run(
        [
            "python",
            str(REDSHIFT_SQL_RUNNER),
            str(sql_file),
        ],
        cwd=str(CLOUD_PLATFORM_ROOT),
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"{failure_message}\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return {
        status_key: "passed",
        "sql_file": str(sql_file),
        "stdout": result.stdout.strip(),
    }


def redshift_create_streaming_landing_table() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_STREAMING_LANDING_TABLE_SQL,
        failure_message=(
            "Redshift Streaming landing table creation failed."
        ),
        status_key=(
            "redshift_streaming_landing_table_creation_status"
        ),
    )


def redshift_copy_streaming_curated_from_s3(
    curated_s3_uri: str,
) -> dict:

    if not curated_s3_uri.startswith("s3://"):
        raise ValueError(
            "Invalid Streaming curated S3 URI: "
            f"{curated_s3_uri!r}"
        )

    return run_redshift_sql_task(
        sql_file=REDSHIFT_COPY_STREAMING_CURATED_SQL,
        failure_message=(
            "Redshift Streaming curated COPY from S3 failed."
        ),
        status_key=(
            "redshift_streaming_curated_copy_status"
        ),
        extra_env={
            "STREAMING_CURATED_S3_URI": curated_s3_uri,
        },
    )


def redshift_create_streaming_analytics_views() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_STREAMING_ANALYTICS_VIEWS_SQL,
        failure_message=(
            "Redshift Streaming analytics view creation failed."
        ),
        status_key=(
            "redshift_streaming_analytics_view_creation_status"
        ),
    )


def redshift_validate_streaming_analytics() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_VALIDATE_STREAMING_ANALYTICS_SQL,
        failure_message=(
            "Redshift Streaming analytics validation query failed."
        ),
        status_key=(
            "redshift_streaming_analytics_validation_status"
        ),
    )


def run_streaming_cross_layer_validation(
    window_id: str,
    curated_s3_uri: str,
) -> None:
    curated_s3_location = (
        curated_s3_uri.rsplit("/", 1)[0] + "/"
    )

    environment = {
        **os.environ,
        "PYTHONPATH": str(CLOUD_PLATFORM_ROOT),
        "STREAMING_WINDOW_ID": window_id,
        "STREAMING_CURATED_S3_LOCATION": curated_s3_location,
    }

    result = subprocess.run(
        [
            "python",
            "-m",
            "scripts.validation.run_streaming_cross_layer_validation",
        ],
        cwd=str(CLOUD_PLATFORM_ROOT),
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Streaming cross-layer validation failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    print(result.stdout)


def publish_latest_streaming_pointer(
    window_id: str,
    curated_s3_uri: str,
) -> str:
    s3_prefix = "s3://"

    if not curated_s3_uri.startswith(s3_prefix):
        raise ValueError(
            "Invalid Streaming curated S3 URI: "
            f"{curated_s3_uri!r}"
        )

    s3_path = curated_s3_uri[len(s3_prefix):]

    bucket_name, events_s3_key = s3_path.split(
        "/",
        maxsplit=1,
    )

    if not bucket_name or not events_s3_key:
        raise ValueError(
            "Streaming curated S3 URI is incomplete: "
            f"{curated_s3_uri!r}"
        )

    result = subprocess.run(
        [
            "python",
            str(STREAMING_LATEST_POINTER_SCRIPT),
            "--window-id",
            window_id,
            "--events-s3-key",
            events_s3_key,
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
            "Latest Streaming pointer publication failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    print(result.stdout)

    return result.stdout.strip()


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

    convert_curated_task = PythonOperator(
        task_id="convert_streaming_window_to_curated",
        python_callable=convert_streaming_window_to_curated,
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

    upload_curated_task = PythonOperator(
        task_id="upload_streaming_window_curated",
        python_callable=upload_streaming_window_curated,
        op_kwargs={
            "curated_file": (
                "{{ ti.xcom_pull("
                "task_ids='convert_streaming_window_to_curated'"
                ") }}"
            ),
            "window_id": (
                "{{ ti.xcom_pull("
                "task_ids='discover_completed_streaming_window'"
                ")['window_id'] }}"
            ),
        },
    )

    redshift_create_streaming_landing_table_task = PythonOperator(
        task_id="redshift_create_streaming_landing_table",
        python_callable=redshift_create_streaming_landing_table,
    )

    redshift_copy_streaming_curated_from_s3_task = PythonOperator(
        task_id="redshift_copy_streaming_curated_from_s3",
        python_callable=redshift_copy_streaming_curated_from_s3,
        op_kwargs={
            "curated_s3_uri": (
                "{{ ti.xcom_pull("
                "task_ids='upload_streaming_window_curated'"
                ") }}"
            ),
        },
    )

    redshift_create_streaming_analytics_views_task = PythonOperator(
        task_id="redshift_create_streaming_analytics_views",
        python_callable=redshift_create_streaming_analytics_views,
    )

    redshift_validate_streaming_analytics_task = PythonOperator(
        task_id="redshift_validate_streaming_analytics",
        python_callable=redshift_validate_streaming_analytics,
    )

    streaming_cross_layer_validation_task = PythonOperator(
        task_id="validate_streaming_cross_layer",
        python_callable=run_streaming_cross_layer_validation,
        op_kwargs={
            "window_id": (
                "{{ ti.xcom_pull("
                "task_ids='discover_completed_streaming_window'"
                ")['window_id'] }}"
            ),
            "curated_s3_uri": (
                "{{ ti.xcom_pull("
                "task_ids='upload_streaming_window_curated'"
                ") }}"
            ),
        },
    )

    publish_latest_streaming_pointer_task = PythonOperator(
        task_id="publish_latest_streaming_pointer",
        python_callable=publish_latest_streaming_pointer,
        op_kwargs={
            "window_id": (
                "{{ ti.xcom_pull("
                "task_ids='discover_completed_streaming_window'"
                ")['window_id'] }}"
            ),
            "curated_s3_uri": (
                "{{ ti.xcom_pull("
                "task_ids='upload_streaming_window_curated'"
                ") }}"
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
            >> convert_curated_task
            >> upload_curated_task
            >> redshift_create_streaming_landing_table_task
            >> redshift_copy_streaming_curated_from_s3_task
            >> redshift_create_streaming_analytics_views_task
            >> redshift_validate_streaming_analytics_task
            >> streaming_cross_layer_validation_task
            >> publish_latest_streaming_pointer_task
            >> mark_processed_task
    )