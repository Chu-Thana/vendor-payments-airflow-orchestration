from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airflow import DAG

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator


BATCH_ETL_ROOT = Path("/opt/airflow/vendor_payments_batch_etl")
STREAMING_PIPELINE_ROOT = Path("/opt/airflow/vendor_payments_streaming")
CLOUD_PLATFORM_ROOT = Path("/opt/airflow/vendor_payments_cloud_platform")
ORCHESTRATION_OUTPUT_ROOT = Path("/opt/airflow/output")

BATCH_ETL_PIPELINE_SCRIPT = BATCH_ETL_ROOT / "scripts/pipeline/run_pipeline.py"
SILVER_OUTPUT = BATCH_ETL_ROOT / "data/processed/silver/vendor_payments_silver.csv"
GOLD_OUTPUT_DIR = BATCH_ETL_ROOT / "data/processed/gold"

BATCH_GOLD_UPLOAD_SCRIPT = CLOUD_PLATFORM_ROOT/ "scripts"/ "batch"/ "upload_full_gold_to_s3.py"

REDSHIFT_SQL_RUNNER = CLOUD_PLATFORM_ROOT / "scripts" / "warehouse" / "run_redshift_sql.py"
REDSHIFT_CREATE_SCHEMAS_SQL = CLOUD_PLATFORM_ROOT/ "sql"/ "redshift"/ "01_create_schemas.sql"
REDSHIFT_CREATE_BATCH_TABLES_SQL = CLOUD_PLATFORM_ROOT/ "sql"/ "redshift"/ "02_create_batch_landing_tables.sql"
REDSHIFT_COPY_BATCH_GOLD_SQL = CLOUD_PLATFORM_ROOT/ "sql"/ "redshift"/ "03_copy_batch_gold_from_s3.sql"
REDSHIFT_CREATE_BATCH_ANALYTICS_VIEWS_SQL = CLOUD_PLATFORM_ROOT/ "sql"/ "redshift"/ "04_create_batch_analytics_views.sql"
REDSHIFT_VALIDATE_BATCH_ANALYTICS_SQL = CLOUD_PLATFORM_ROOT / "sql" / "redshift" / "05_validate_batch_analytics.sql"

STREAMING_STAGING_OUTPUT = STREAMING_PIPELINE_ROOT / "output/staging/vendor_payments_streaming_staging.jsonl"
STREAMING_CURATED_CONVERTER_SCRIPT = CLOUD_PLATFORM_ROOT/ "scripts"/ "streaming" / "convert_streaming_jsonl_to_csv.py"
STREAMING_CURATED_UPLOAD_SCRIPT = CLOUD_PLATFORM_ROOT/ "scripts" / "streaming"  / "upload_streaming_curated_to_s3.py"
STREAMING_REPORTS_UPLOAD_SCRIPT = CLOUD_PLATFORM_ROOT/ "scripts"/ "streaming"/ "upload_streaming_reports_to_s3.py"

REDSHIFT_CREATE_STREAMING_LANDING_TABLE_SQL = CLOUD_PLATFORM_ROOT / "sql" / "redshift" / "06_create_streaming_landing_table.sql"
REDSHIFT_COPY_STREAMING_CURATED_SQL = CLOUD_PLATFORM_ROOT/ "sql"/ "redshift"/ "07_copy_streaming_curated_from_s3.sql"
REDSHIFT_CREATE_STREAMING_ANALYTICS_VIEWS_SQL = CLOUD_PLATFORM_ROOT / "sql" / "redshift" / "08_create_streaming_analytics_views.sql"
REDSHIFT_VALIDATE_STREAMING_ANALYTICS_SQL = CLOUD_PLATFORM_ROOT/ "sql"/ "redshift"/ "09_validate_streaming_analytics.sql"


REDSHIFT_SUMMARY_SCRIPT = CLOUD_PLATFORM_ROOT / "scripts/warehouse/generate_redshift_summary.py"
REDSHIFT_EXECUTION_SUMMARY =  CLOUD_PLATFORM_ROOT / "output/reports/redshift_execution_summary.json"
ORCHESTRATION_SUMMARY =  ORCHESTRATION_OUTPUT_ROOT / "reports/airflow_orchestration_summary.json"

def check_batch_etl_ready() -> None:
    if not BATCH_ETL_ROOT.exists():
        raise FileNotFoundError(f"Batch ETL root not found: {BATCH_ETL_ROOT}")

    if not BATCH_ETL_PIPELINE_SCRIPT.exists():
        raise FileNotFoundError(
            f"Batch ETL pipeline script not found: {BATCH_ETL_PIPELINE_SCRIPT}"
        )


def run_batch_etl_pipeline() -> None:
    result = subprocess.run(
        ["python", "-m", "scripts.pipeline.run_pipeline"],
        cwd=str(BATCH_ETL_ROOT),
        env={
            **os.environ,
            "PYTHONPATH": str(BATCH_ETL_ROOT),
        },
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Batch ETL pipeline failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )



def validate_silver_output() -> dict:
    if not SILVER_OUTPUT.exists():
        raise FileNotFoundError(f"Silver output not found: {SILVER_OUTPUT}")

    file_size_bytes = SILVER_OUTPUT.stat().st_size

    if file_size_bytes == 0:
        raise ValueError(f"Silver output is empty: {SILVER_OUTPUT}")

    return {
        "silver_output": str(SILVER_OUTPUT),
        "silver_file_size_bytes": file_size_bytes,
        "silver_validation_status": "passed",
    }


def validate_gold_outputs() -> dict:
    if not GOLD_OUTPUT_DIR.exists():
        raise FileNotFoundError(f"Gold output directory not found: {GOLD_OUTPUT_DIR}")

    gold_files = sorted(GOLD_OUTPUT_DIR.glob("*.csv"))

    if not gold_files:
        raise FileNotFoundError(f"No gold CSV files found in: {GOLD_OUTPUT_DIR}")

    gold_summary = {
        gold_file.name: gold_file.stat().st_size
        for gold_file in gold_files
    }

    empty_files = [
        file_name
        for file_name, file_size in gold_summary.items()
        if file_size == 0
    ]

    if empty_files:
        raise ValueError(f"Empty gold output files found: {empty_files}")

    return {
        "gold_output_dir": str(GOLD_OUTPUT_DIR),
        "gold_files": gold_summary,
        "gold_validation_status": "passed",
    }


def upload_batch_gold_to_s3() -> dict:
    result = subprocess.run(
        [
            "python",
            str(BATCH_GOLD_UPLOAD_SCRIPT),
        ],
        cwd=str(CLOUD_PLATFORM_ROOT),
        env={
            **os.environ,
            "PYTHONPATH": str(CLOUD_PLATFORM_ROOT),
            "VENDOR_BATCH_ETL_CONTAINER_PATH": str(BATCH_ETL_ROOT),
            "S3_BUCKET": os.environ["S3_BUCKET"],
            "S3_PREFIX": os.environ["S3_PREFIX"],
        },
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Batch Gold upload to S3 failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return {
        "upload_status": "passed",
        "stdout": result.stdout.strip(),
    }

def check_streaming_staging_ready() -> dict:
    if not STREAMING_STAGING_OUTPUT.exists():
        raise FileNotFoundError(
            "Streaming staging output not found: "
            f"{STREAMING_STAGING_OUTPUT}"
        )

    file_size_bytes = STREAMING_STAGING_OUTPUT.stat().st_size

    if file_size_bytes == 0:
        raise ValueError(
            "Streaming staging output is empty: "
            f"{STREAMING_STAGING_OUTPUT}"
        )

    return {
        "streaming_staging_output": str(
            STREAMING_STAGING_OUTPUT
        ),
        "streaming_staging_file_size_bytes": file_size_bytes,
        "streaming_staging_status": "passed",
    }


def run_downstream_deduplication_check() -> dict:
    total_records = 0
    event_ids: set[str] = set()
    duplicate_event_ids = 0
    missing_event_ids = 0

    with STREAMING_STAGING_OUTPUT.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            record = json.loads(line)
            total_records += 1

            event_id = record.get("event_id")

            if not event_id:
                missing_event_ids += 1
                continue

            if event_id in event_ids:
                duplicate_event_ids += 1
            else:
                event_ids.add(event_id)

    if total_records == 0:
        raise ValueError("Streaming staging output contains zero records.")

    return {
        "total_staging_records": total_records,
        "unique_event_ids": len(event_ids),
        "duplicate_event_ids": duplicate_event_ids,
        "missing_event_ids": missing_event_ids,
        "downstream_deduplication_status": (
            "passed"
            if duplicate_event_ids == 0
            else "duplicates_detected"
        ),
        "deduplication_layer": "airflow_downstream_validation",
        "principle": (
            "Prevent data loss first, "
            "then handle duplicates downstream."
        ),
    }


def convert_streaming_jsonl_to_csv() -> dict:
    result = subprocess.run(
        [
            "python",
            str(STREAMING_CURATED_CONVERTER_SCRIPT),
        ],
        cwd=str(CLOUD_PLATFORM_ROOT),
        env={
            **os.environ,
            "PYTHONPATH": str(CLOUD_PLATFORM_ROOT),
            "VENDOR_STREAMING_CONTAINER_PATH": str(
                STREAMING_PIPELINE_ROOT
            ),
        },
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Streaming JSONL to CSV conversion failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return {
        "streaming_curated_conversion_status": "passed",
        "stdout": result.stdout.strip(),
    }


def upload_streaming_curated_to_s3() -> dict:
    result = subprocess.run(
        [
            "python",
            str(STREAMING_CURATED_UPLOAD_SCRIPT),
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
            "Streaming curated upload to S3 failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return {
        "streaming_curated_upload_status": "passed",
        "stdout": result.stdout.strip(),
    }


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

def run_redshift_sql_task(
    sql_file: Path,
    failure_message: str,
    status_key: str,
) -> dict:
    result = subprocess.run(
        [
            "python",
            str(REDSHIFT_SQL_RUNNER),
            str(sql_file),
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


def redshift_create_schemas() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_SCHEMAS_SQL,
        failure_message=(
            "Redshift schema creation failed."
        ),
        status_key=(
            "redshift_schema_creation_status"
        ),
    )


def redshift_create_batch_landing_tables() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_BATCH_TABLES_SQL,
        failure_message=(
            "Redshift Batch landing table "
            "creation failed."
        ),
        status_key=(
            "redshift_batch_landing_table_"
            "creation_status"
        ),
    )


def redshift_copy_batch_gold_from_s3() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_COPY_BATCH_GOLD_SQL,
        failure_message=(
            "Redshift Batch Gold COPY "
            "from S3 failed."
        ),
        status_key=(
            "redshift_batch_gold_copy_status"
        ),
    )


def redshift_create_batch_analytics_views() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_BATCH_ANALYTICS_VIEWS_SQL,
        failure_message=(
            "Redshift Batch analytics view "
            "creation failed."
        ),
        status_key=(
            "redshift_batch_analytics_view_"
            "creation_status"
        ),
    )


def redshift_validate_batch_analytics() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_VALIDATE_BATCH_ANALYTICS_SQL,
        failure_message=(
            "Redshift Batch analytics validation "
            "query failed."
        ),
        status_key=(
            "redshift_batch_analytics_validation_status"
        ),
    )


def redshift_create_streaming_landing_table() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_STREAMING_LANDING_TABLE_SQL,
        failure_message=(
            "Redshift Streaming landing table "
            "creation failed."
        ),
        status_key=(
            "redshift_streaming_landing_table_"
            "creation_status"
        ),
    )


def redshift_copy_streaming_curated_from_s3() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_COPY_STREAMING_CURATED_SQL,
        failure_message=(
            "Redshift Streaming curated COPY "
            "from S3 failed."
        ),
        status_key=(
            "redshift_streaming_curated_copy_status"
        ),
    )


def redshift_create_streaming_analytics_views() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_STREAMING_ANALYTICS_VIEWS_SQL,
        failure_message=(
            "Redshift Streaming analytics view "
            "creation failed."
        ),
        status_key=(
            "redshift_streaming_analytics_view_"
            "creation_status"
        ),
    )


def redshift_validate_streaming_analytics() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_VALIDATE_STREAMING_ANALYTICS_SQL,
        failure_message=(
            "Redshift Streaming analytics validation "
            "query failed."
        ),
        status_key=(
            "redshift_streaming_analytics_validation_status"
        ),
    )


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

    silver_validation = task_instance.xcom_pull(
        task_ids="validate_silver_output"
    )
    gold_validation = task_instance.xcom_pull(
        task_ids="validate_gold_outputs"
    )
    staging_validation = task_instance.xcom_pull(
        task_ids="check_streaming_staging_ready"
    )
    deduplication_check = task_instance.xcom_pull(
        task_ids="run_downstream_deduplication_check"
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
        "project": "Vendor Payments Airflow Orchestration",
        "pipeline_version": "1.0.0",
        "dag_id": "vendor_payments_data_platform_orchestration",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_pipeline": {
            "batch_etl_status": "completed",
            "silver_validation": silver_validation,
            "gold_validation": gold_validation,
        },
        "streaming_pipeline": {
            "streaming_staging_validation": staging_validation,
            "downstream_deduplication_check": deduplication_check,
        },
        "cloud_pipeline": {
            "cloud_platform_readiness": cloud_platform_readiness,
            "redshift_summary_generation": redshift_generation,
        },
        "redshift_pipeline": redshift_validation,
        "orchestration_status": "completed",
        "validation": {
            "status": "PASS",
        },
        "design_note": (
            "Airflow orchestrates Batch ETL, validates streaming "
            "staging output, performs downstream deduplication checks, "
            "and validates Amazon Redshift Serverless runtime metadata."
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


def upload_streaming_reports_to_s3() -> dict:
    result = subprocess.run(
        [
            "python",
            str(STREAMING_REPORTS_UPLOAD_SCRIPT),
        ],
        cwd=str(CLOUD_PLATFORM_ROOT),
        env={
            **os.environ,
            "PYTHONPATH": str(CLOUD_PLATFORM_ROOT),
            "STREAMING_PIPELINE_ROOT": str(
                STREAMING_PIPELINE_ROOT
            ),
            "ORCHESTRATION_OUTPUT_ROOT": str(
                ORCHESTRATION_OUTPUT_ROOT
            ),
        },
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Streaming reports upload to S3 failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return {
        "streaming_reports_upload_status": "passed",
        "stdout": result.stdout.strip(),
    }


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
    start = EmptyOperator(task_id="start")

    check_batch_etl_ready_task = PythonOperator(
        task_id="check_batch_etl_ready",
        python_callable=check_batch_etl_ready,
    )

    run_batch_etl_pipeline_task = PythonOperator(
        task_id="run_batch_etl_pipeline",
        python_callable=run_batch_etl_pipeline,
    )

    validate_silver_output_task = PythonOperator(
        task_id="validate_silver_output",
        python_callable=validate_silver_output,
    )

    validate_gold_outputs_task = PythonOperator(
        task_id="validate_gold_outputs",
        python_callable=validate_gold_outputs,
    )

    upload_batch_gold_to_s3_task = PythonOperator(
        task_id="upload_batch_gold_to_s3",
        python_callable=upload_batch_gold_to_s3,
    )

    check_streaming_staging_ready_task = PythonOperator(
        task_id="check_streaming_staging_ready",
        python_callable=check_streaming_staging_ready,
    )

    run_downstream_deduplication_check_task = PythonOperator(
        task_id="run_downstream_deduplication_check",
        python_callable=run_downstream_deduplication_check,
    )

    convert_streaming_jsonl_to_csv_task = PythonOperator(
        task_id="convert_streaming_jsonl_to_csv",
        python_callable=convert_streaming_jsonl_to_csv,
    )

    upload_streaming_curated_to_s3_task = PythonOperator(
        task_id="upload_streaming_curated_to_s3",
        python_callable=upload_streaming_curated_to_s3,
    )

    check_cloud_platform_ready_task = PythonOperator(
        task_id="check_cloud_platform_ready",
        python_callable=check_cloud_platform_ready,
    )

    redshift_create_schemas_task = PythonOperator(
        task_id="redshift_create_schemas",
        python_callable=redshift_create_schemas,
    )

    redshift_create_batch_landing_tables_task = PythonOperator(
        task_id="redshift_create_batch_landing_tables",
        python_callable=redshift_create_batch_landing_tables,
    )

    redshift_copy_batch_gold_from_s3_task = PythonOperator(
        task_id="redshift_copy_batch_gold_from_s3",
        python_callable=redshift_copy_batch_gold_from_s3,
    )

    redshift_create_batch_analytics_views_task = PythonOperator(
        task_id="redshift_create_batch_analytics_views",
        python_callable=redshift_create_batch_analytics_views,
    )

    redshift_validate_batch_analytics_task = PythonOperator(
        task_id="redshift_validate_batch_analytics",
        python_callable=redshift_validate_batch_analytics,
    )

    redshift_create_streaming_landing_table_task = PythonOperator(
        task_id="redshift_create_streaming_landing_table",
        python_callable=redshift_create_streaming_landing_table,
    )

    redshift_copy_streaming_curated_from_s3_task = PythonOperator(
        task_id="redshift_copy_streaming_curated_from_s3",
        python_callable=redshift_copy_streaming_curated_from_s3,
    )

    redshift_create_streaming_analytics_views_task = PythonOperator(
        task_id="redshift_create_streaming_analytics_views",
        python_callable=redshift_create_streaming_analytics_views,
    )

    redshift_validate_streaming_analytics_task = PythonOperator(
        task_id="redshift_validate_streaming_analytics",
        python_callable=redshift_validate_streaming_analytics,
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

    upload_streaming_reports_to_s3_task = PythonOperator(
        task_id="upload_streaming_reports_to_s3",
        python_callable=upload_streaming_reports_to_s3,
    )

    end = EmptyOperator(task_id="end")

    (
            start
            >> check_batch_etl_ready_task
            >> run_batch_etl_pipeline_task
            >> validate_silver_output_task
            >> validate_gold_outputs_task
            >> upload_batch_gold_to_s3_task
            >> check_streaming_staging_ready_task
            >> run_downstream_deduplication_check_task
            >> convert_streaming_jsonl_to_csv_task
            >> upload_streaming_curated_to_s3_task
            >> check_cloud_platform_ready_task
            >> redshift_create_schemas_task
    )

    (
            redshift_create_schemas_task
            >> redshift_create_batch_landing_tables_task
            >> redshift_copy_batch_gold_from_s3_task
            >> redshift_create_batch_analytics_views_task
            >> redshift_validate_batch_analytics_task
    )

    (
            redshift_create_schemas_task
            >> redshift_create_streaming_landing_table_task
            >> redshift_copy_streaming_curated_from_s3_task
            >> redshift_create_streaming_analytics_views_task
            >> redshift_validate_streaming_analytics_task
    )

    [
        redshift_validate_batch_analytics_task,
        redshift_validate_streaming_analytics_task,
    ] >> generate_redshift_execution_summary_task

    (
            generate_redshift_execution_summary_task
            >> validate_redshift_execution_summary_task
            >> generate_orchestration_summary_task
            >> upload_streaming_reports_to_s3_task
            >> end
    )