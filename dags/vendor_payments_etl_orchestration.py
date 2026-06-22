from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator


PROJECT1_ROOT = Path("/opt/airflow/project1")
PROJECT3_ROOT = Path("/opt/airflow/project3")
PROJECT5_ROOT = Path("/opt/airflow/project5")
PROJECT4_OUTPUT_ROOT = Path("/opt/airflow/output")

PROJECT1_PIPELINE_SCRIPT = PROJECT1_ROOT / "scripts/pipeline/run_pipeline.py"

SILVER_OUTPUT = (
    PROJECT1_ROOT
    / "data/processed/silver/vendor_payments_silver.csv"
)

GOLD_OUTPUT_DIR = PROJECT1_ROOT / "data/processed/gold"

PROJECT3_STAGING_OUTPUT = (
    PROJECT3_ROOT
    / "output/staging/vendor_payments_streaming_staging.jsonl"
)

PROJECT5_REDSHIFT_SUMMARY_SCRIPT = (
    PROJECT5_ROOT
    / "scripts/warehouse/generate_redshift_summary.py"
)

PROJECT5_REDSHIFT_SUMMARY = (
    PROJECT5_ROOT
    / "output/reports/redshift_execution_summary.json"
)

ORCHESTRATION_SUMMARY = (
    PROJECT4_OUTPUT_ROOT
    / "reports/airflow_orchestration_summary.json"
)


def check_project1_ready() -> None:
    if not PROJECT1_ROOT.exists():
        raise FileNotFoundError(f"Project 1 root not found: {PROJECT1_ROOT}")

    if not PROJECT1_PIPELINE_SCRIPT.exists():
        raise FileNotFoundError(
            f"Project 1 pipeline script not found: {PROJECT1_PIPELINE_SCRIPT}"
        )


def run_project1_pipeline() -> None:
    result = subprocess.run(
        ["python", "-m", "scripts.pipeline.run_pipeline"],
        cwd=str(PROJECT1_ROOT),
        env={
            **os.environ,
            "PYTHONPATH": str(PROJECT1_ROOT),
        },
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Project 1 pipeline failed.\n"
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


def check_project3_streaming_staging() -> dict:
    if not PROJECT3_STAGING_OUTPUT.exists():
        raise FileNotFoundError(
            f"Project 3 staging output not found: {PROJECT3_STAGING_OUTPUT}"
        )

    file_size_bytes = PROJECT3_STAGING_OUTPUT.stat().st_size

    if file_size_bytes == 0:
        raise ValueError(
            f"Project 3 staging output is empty: {PROJECT3_STAGING_OUTPUT}"
        )

    return {
        "project3_staging_output": str(PROJECT3_STAGING_OUTPUT),
        "project3_staging_file_size_bytes": file_size_bytes,
        "project3_staging_status": "passed",
    }


def run_downstream_deduplication_check() -> dict:
    total_records = 0
    event_ids: set[str] = set()
    duplicate_event_ids = 0
    missing_event_ids = 0

    with PROJECT3_STAGING_OUTPUT.open("r", encoding="utf-8") as file:
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
        raise ValueError("Project 3 staging output contains zero records.")

    return {
        "total_staging_records": total_records,
        "unique_event_ids": len(event_ids),
        "duplicate_event_ids": duplicate_event_ids,
        "missing_event_ids": missing_event_ids,
        "downstream_deduplication_status": "passed"
        if duplicate_event_ids == 0
        else "duplicates_detected",
        "deduplication_layer": "airflow_downstream_validation",
        "principle": "Prevent data loss first, then handle duplicates downstream.",
    }


def check_project5_ready() -> dict:
    if not PROJECT5_ROOT.exists():
        raise FileNotFoundError(
            f"Project 5 root not found: {PROJECT5_ROOT}"
        )

    if not PROJECT5_REDSHIFT_SUMMARY_SCRIPT.exists():
        raise FileNotFoundError(
            "Project 5 Redshift summary script not found: "
            f"{PROJECT5_REDSHIFT_SUMMARY_SCRIPT}"
        )

    return {
        "project5_root": str(PROJECT5_ROOT),
        "redshift_summary_script": str(
            PROJECT5_REDSHIFT_SUMMARY_SCRIPT
        ),
        "project5_readiness_status": "passed",
    }


def generate_redshift_execution_summary() -> dict:
    result = subprocess.run(
        [
            "python",
            str(PROJECT5_REDSHIFT_SUMMARY_SCRIPT),
        ],
        cwd=str(PROJECT5_ROOT),
        env={
            **os.environ,
            "PYTHONPATH": str(PROJECT5_ROOT),
        },
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Project 5 Redshift summary generation failed.\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    if not PROJECT5_REDSHIFT_SUMMARY.exists():
        raise FileNotFoundError(
            "Redshift execution summary was not generated: "
            f"{PROJECT5_REDSHIFT_SUMMARY}"
        )

    return {
        "summary_file": str(PROJECT5_REDSHIFT_SUMMARY),
        "generation_status": "passed",
        "stdout": result.stdout.strip(),
    }


def validate_redshift_execution_summary() -> dict:
    if not PROJECT5_REDSHIFT_SUMMARY.exists():
        raise FileNotFoundError(
            "Redshift execution summary not found: "
            f"{PROJECT5_REDSHIFT_SUMMARY}"
        )

    with PROJECT5_REDSHIFT_SUMMARY.open(
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
        "summary_file": str(PROJECT5_REDSHIFT_SUMMARY),
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
        task_ids="check_project3_streaming_staging"
    )
    deduplication_check = task_instance.xcom_pull(
        task_ids="run_downstream_deduplication_check"
    )
    project5_readiness = task_instance.xcom_pull(
        task_ids="check_project5_ready"
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
            "project1_status": "completed",
            "silver_validation": silver_validation,
            "gold_validation": gold_validation,
        },
        "streaming_pipeline": {
            "project3_staging_validation": staging_validation,
            "downstream_deduplication_check": deduplication_check,
        },
        "cloud_pipeline": {
            "project5_readiness": project5_readiness,
            "redshift_summary_generation": redshift_generation,
        },
        "redshift_pipeline": redshift_validation,
        "orchestration_status": "completed",
        "validation": {
            "status": "PASS",
        },
        "design_note": (
            "Airflow orchestrates Project 1 Batch ETL, validates "
            "Project 3 streaming staging output, performs downstream "
            "deduplication checks, and validates Project 5 Amazon "
            "Redshift Serverless runtime metadata."
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
    tags=[
        "vendor-payments",
        "batch",
        "streaming",
        "redshift",
        "orchestration",
    ],
) as dag:
    start = EmptyOperator(task_id="start")

    check_project1_ready_task = PythonOperator(
        task_id="check_project1_ready",
        python_callable=check_project1_ready,
    )

    run_project1_pipeline_task = PythonOperator(
        task_id="run_project1_pipeline",
        python_callable=run_project1_pipeline,
    )

    validate_silver_output_task = PythonOperator(
        task_id="validate_silver_output",
        python_callable=validate_silver_output,
    )

    validate_gold_outputs_task = PythonOperator(
        task_id="validate_gold_outputs",
        python_callable=validate_gold_outputs,
    )

    check_project3_streaming_staging_task = PythonOperator(
        task_id="check_project3_streaming_staging",
        python_callable=check_project3_streaming_staging,
    )

    run_downstream_deduplication_check_task = PythonOperator(
        task_id="run_downstream_deduplication_check",
        python_callable=run_downstream_deduplication_check,
    )

    check_project5_ready_task = PythonOperator(
        task_id="check_project5_ready",
        python_callable=check_project5_ready,
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

    end = EmptyOperator(task_id="end")

    (
        start
        >> check_project1_ready_task
        >> run_project1_pipeline_task
        >> validate_silver_output_task
        >> validate_gold_outputs_task
        >> check_project3_streaming_staging_task
        >> run_downstream_deduplication_check_task
        >> check_project5_ready_task
        >> generate_redshift_execution_summary_task
        >> validate_redshift_execution_summary_task
        >> generate_orchestration_summary_task
        >> end
    )