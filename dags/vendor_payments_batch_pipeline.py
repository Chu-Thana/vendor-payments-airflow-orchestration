import os
import subprocess

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator

import psycopg2

BATCH_ETL_PATH = os.getenv(
    "VENDOR_BATCH_ETL_CONTAINER_PATH",
    "/opt/airflow/vendor_payments_batch_etl",
)

BATCH_TRANSFORM_SILVER_SCRIPT = (
    f"{BATCH_ETL_PATH}/scripts/pipeline/transform_silver.py"
)

BATCH_BUILD_GOLD_SCRIPT = (
    f"{BATCH_ETL_PATH}/scripts/pipeline/build_gold_marts.py"
)

BATCH_CHECK_SILVER_SCRIPT = (
    f"{BATCH_ETL_PATH}/scripts/checks/check_silver_output.py"
)

BATCH_CHECK_GOLD_SCRIPT = (
    f"{BATCH_ETL_PATH}/scripts/checks/check_gold_outputs.py"
)

BATCH_SILVER_CANDIDATE_FILE = (
    Path(BATCH_ETL_PATH)
    / "data"
    / "processed"
    / "silver"
    / "vendor_payments_silver.candidate.csv"
)

BATCH_SILVER_FINAL_FILE = (
    Path(BATCH_ETL_PATH)
    / "data"
    / "processed"
    / "silver"
    / "vendor_payments_silver.csv"
)

CLOUD_PLATFORM_ROOT = Path(
    "/opt/airflow/vendor_payments_cloud_platform"
)

BATCH_GOLD_UPLOAD_SCRIPT = (
    CLOUD_PLATFORM_ROOT
    / "scripts"
    / "batch"
    / "upload_full_gold_to_s3.py"
)

REDSHIFT_SQL_RUNNER = (
    CLOUD_PLATFORM_ROOT
    / "scripts"
    / "warehouse"
    / "run_redshift_sql.py"
)

REDSHIFT_CREATE_SCHEMAS_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "01_create_schemas.sql"
)

REDSHIFT_CREATE_BATCH_TABLES_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "02_create_batch_landing_tables.sql"
)

REDSHIFT_COPY_BATCH_GOLD_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "03_copy_batch_gold_from_s3.sql"
)

REDSHIFT_CREATE_BATCH_ANALYTICS_VIEWS_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "04_create_batch_analytics_views.sql"
)

REDSHIFT_VALIDATE_BATCH_ANALYTICS_SQL = (
    CLOUD_PLATFORM_ROOT
    / "sql"
    / "redshift"
    / "05_validate_batch_analytics.sql"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

GOLD_ARTIFACT_NAMES = (
    "mart_spending_by_fiscal_year.csv",
    "mart_spending_by_department.csv",
    "mart_spending_by_supplier_top_n.csv",
    "mart_pending_by_department.csv",
    "mart_fund_category_summary.csv",
)


def get_recovery_connection():
    return psycopg2.connect(
        host=os.environ["RECOVERY_DB_HOST"],
        port=int(os.getenv("RECOVERY_DB_PORT", "5432")),
        dbname=os.environ["RECOVERY_DB_NAME"],
        user=os.environ["RECOVERY_DB_USER"],
        password=os.environ["RECOVERY_DB_PASSWORD"],
    )


def get_or_create_recovery_run(
    airflow_run_id: str,
) -> int:
    run_key = f"airflow::{airflow_run_id}"

    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO recovery.pipeline_runs (
                    run_key,
                    pipeline_name,
                    mode,
                    status
                )
                VALUES (
                    %s,
                    'vendor_payments_batch_etl',
                    'FULL',
                    'CREATED'
                )
                ON CONFLICT (run_key)
                DO NOTHING;
                """,
                (run_key,),
            )

            cursor.execute(
                """
                SELECT pipeline_run_id
                FROM recovery.pipeline_runs
                WHERE run_key = %s;
                """,
                (run_key,),
            )

            row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            f"Recovery pipeline run not found: {run_key}"
        )

    return int(row[0])


def run_with_recovery(
    *,
    context: dict,
    stage_name: str,
    operation,
) -> dict:
    dag_run = context["dag_run"]

    pipeline_run_id = get_or_create_recovery_run(
        airflow_run_id=dag_run.run_id,
    )

    stage_execution_id = start_recovery_stage(
        pipeline_run_id=pipeline_run_id,
        stage_name=stage_name,
    )

    try:
        result = operation()

    except Exception as exc:
        finish_recovery_stage(
            stage_execution_id=stage_execution_id,
            success=False,
            error_message=str(exc),
        )
        raise

    finish_recovery_stage(
        stage_execution_id=stage_execution_id,
        success=True,
    )

    return {
        "pipeline_run_id": pipeline_run_id,
        "stage_execution_id": stage_execution_id,
        "stage_name": stage_name,
        "status": "SUCCEEDED",
        "result": result,
    }


def start_recovery_stage(
    pipeline_run_id: int,
    stage_name: str,
    input_dataset_version_id: int | None = None,
    output_dataset_version_id: int | None = None,
) -> int:
    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT recovery.start_stage_attempt(
                    %s,
                    %s
                );
                """,
                (
                    pipeline_run_id,
                    stage_name,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "Stage attempt could not be started."
                )

            stage_execution_id = int(row[0])

            cursor.execute(
                """
                UPDATE recovery.stage_executions
                SET
                    input_dataset_version_id = %s,
                    output_dataset_version_id = %s
                WHERE stage_execution_id = %s;
                """,
                (
                    input_dataset_version_id,
                    output_dataset_version_id,
                    stage_execution_id,
                ),
            )

    return stage_execution_id


def finish_recovery_stage(
    stage_execution_id: int,
    success: bool,
    error_message: str | None = None,
) -> None:
    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT recovery.finish_stage_attempt(
                    %s,
                    %s,
                    %s
                );
                """,
                (
                    stage_execution_id,
                    success,
                    error_message,
                ),
            )


def run_transform_silver_with_recovery(
    **context,
) -> dict:
    dag_run = context["dag_run"]

    pipeline_run_id = get_or_create_recovery_run(
        airflow_run_id=dag_run.run_id,
    )

    dataset_version_id = (
        get_or_create_dataset_version(
            pipeline_run_id=pipeline_run_id,
            layer="SILVER",
        )
    )

    update_dataset_version_status(
        dataset_version_id,
        "BUILDING",
    )

    dataset_artifact_id = (
        get_or_create_dataset_artifact(
            dataset_version_id=dataset_version_id,
            artifact_name="vendor_payments_silver.csv",
            artifact_path=str(
                BATCH_SILVER_FINAL_FILE
            ),
        )
    )

    artifact_execution_id = (
        start_recovery_artifact(
            dataset_artifact_id
        )
    )

    stage_execution_id = start_recovery_stage(
        pipeline_run_id=pipeline_run_id,
        stage_name="TRANSFORM_SILVER",
        output_dataset_version_id=dataset_version_id,
    )

    result = subprocess.run(
        [
            "python",
            "scripts/pipeline/transform_silver.py",
        ],
        cwd=BATCH_ETL_PATH,
        env={
            **os.environ,
            "PYTHONPATH": BATCH_ETL_PATH,
        },
        check=False,
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.returncode != 0:
        error_message = (
            "TRANSFORM_SILVER failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

        finish_recovery_artifact(
            artifact_execution_id,
            False,
            error_message=error_message,
        )

        update_dataset_version_status(
            dataset_version_id,
            "FAILED",
        )

        finish_recovery_stage(
            stage_execution_id,
            False,
            error_message,
        )

        raise RuntimeError(
            error_message
        )

    finish_recovery_artifact(
        artifact_execution_id,
        True,
    )

    update_dataset_version_status(
        dataset_version_id,
        "TRANSFORMED",
    )

    finish_recovery_stage(
        stage_execution_id,
        True,
    )

    return {
        "pipeline_run_id": pipeline_run_id,
        "dataset_version_id": dataset_version_id,
        "dataset_artifact_id": (
            dataset_artifact_id
        ),
        "artifact_execution_id": (
            artifact_execution_id
        ),
        "stage_execution_id": (
            stage_execution_id
        ),
        "stage_name": (
            "TRANSFORM_SILVER"
        ),
        "status": "SUCCEEDED",
    }


def run_check_silver_with_recovery(**context) -> dict:
    dag_run = context["dag_run"]

    pipeline_run_id = get_or_create_recovery_run(
        airflow_run_id=dag_run.run_id,
    )

    dataset_version_id = (
        get_or_create_dataset_version(
            pipeline_run_id=pipeline_run_id,
            layer="SILVER",
        )
    )

    stage_execution_id = start_recovery_stage(
        pipeline_run_id=pipeline_run_id,
        stage_name="CHECK_SILVER",
        input_dataset_version_id=dataset_version_id,
        output_dataset_version_id=dataset_version_id,
    )

    try:
        validation_result = validate_silver_output()

    except Exception as exc:
        update_dataset_version_status(
            dataset_version_id,
            "FAILED",
        )

        finish_recovery_stage(
            stage_execution_id=stage_execution_id,
            success=False,
            error_message=str(exc),
        )

        raise

    update_dataset_version_status(
        dataset_version_id,
        "PUBLISHED",
    )

    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE recovery.dataset_artifacts
                SET status = 'PUBLISHED'
                WHERE dataset_version_id = %s
                  AND artifact_name =
                      'vendor_payments_silver.csv';
                """,
                (dataset_version_id,),
            )

    finish_recovery_stage(
        stage_execution_id=stage_execution_id,
        success=True,
    )

    return {
        "pipeline_run_id": pipeline_run_id,
        "dataset_version_id": dataset_version_id,
        "stage_execution_id": stage_execution_id,
        "stage_name": "CHECK_SILVER",
        "status": "SUCCEEDED",
        "validation_result": validation_result,
    }


def get_or_create_dataset_version(
    pipeline_run_id: int,
    layer: str,
) -> int:
    version_key = (
        f"{layer.lower()}_pipeline_run_{pipeline_run_id}"
    )

    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO recovery.dataset_versions (
                    pipeline_run_id,
                    version_key,
                    layer,
                    status
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    'CREATED'
                )
                ON CONFLICT (
                    pipeline_run_id,
                    layer
                )
                DO NOTHING;
                """,
                (
                    pipeline_run_id,
                    version_key,
                    layer,
                ),
            )

            cursor.execute(
                """
                SELECT dataset_version_id
                FROM recovery.dataset_versions
                WHERE pipeline_run_id = %s
                  AND layer = %s;
                """,
                (
                    pipeline_run_id,
                    layer,
                ),
            )

            row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            "Dataset version could not be created "
            f"for pipeline_run_id={pipeline_run_id}, "
            f"layer={layer}."
        )

    return int(row[0])


def update_dataset_version_status(
    dataset_version_id: int,
    status: str,
    row_count: int | None = None,
) -> None:
    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE recovery.dataset_versions
                SET
                    status = %s,
                    row_count = COALESCE(
                        %s,
                        row_count
                    ),
                    validated_at = CASE
                        WHEN %s IN (
                            'VALIDATED',
                            'PUBLISHED'
                        )
                        THEN clock_timestamp()
                        ELSE validated_at
                    END,
                    published_at = CASE
                        WHEN %s = 'PUBLISHED'
                        THEN clock_timestamp()
                        ELSE published_at
                    END
                WHERE dataset_version_id = %s;
                """,
                (
                    status,
                    row_count,
                    status,
                    status,
                    dataset_version_id,
                ),
            )


def get_or_create_dataset_artifact(
    dataset_version_id: int,
    artifact_name: str,
    artifact_path: str,
) -> int:
    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO recovery.dataset_artifacts (
                    dataset_version_id,
                    artifact_name,
                    artifact_path,
                    status
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    'CREATED'
                )
                ON CONFLICT (
                    dataset_version_id,
                    artifact_name
                )
                DO NOTHING;
                """,
                (
                    dataset_version_id,
                    artifact_name,
                    artifact_path,
                ),
            )

            cursor.execute(
                """
                SELECT dataset_artifact_id
                FROM recovery.dataset_artifacts
                WHERE dataset_version_id = %s
                  AND artifact_name = %s;
                """,
                (
                    dataset_version_id,
                    artifact_name,
                ),
            )

            row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            f"Dataset artifact could not be created: "
            f"{artifact_name}"
        )

    return int(row[0])


def start_recovery_artifact(
    dataset_artifact_id: int,
) -> int:
    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT recovery.start_artifact_attempt(
                    %s
                );
                """,
                (dataset_artifact_id,),
            )

            row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            "Artifact attempt could not be started."
        )

    return int(row[0])


def finish_recovery_artifact(
    artifact_execution_id: int,
    success: bool,
    row_count: int | None = None,
    checksum: str | None = None,
    error_message: str | None = None,
) -> None:
    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT recovery.finish_artifact_attempt(
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                );
                """,
                (
                    artifact_execution_id,
                    success,
                    row_count,
                    checksum,
                    error_message,
                ),
            )


def get_or_create_gold_artifacts(
    dataset_version_id: int,
) -> dict[str, int]:
    gold_candidate_dir = (
        Path(BATCH_ETL_PATH)
        / "data"
        / "processed"
        / "gold_candidate"
    )

    artifact_ids: dict[str, int] = {}

    for artifact_name in GOLD_ARTIFACT_NAMES:
        artifact_id = get_or_create_dataset_artifact(
            dataset_version_id=dataset_version_id,
            artifact_name=artifact_name,
            artifact_path=str(
                gold_candidate_dir / artifact_name
            ),
        )

        artifact_ids[artifact_name] = artifact_id

    return artifact_ids


def validate_silver_output() -> dict:
    command = f"""
import sys
from pathlib import Path

batch_root = Path({BATCH_ETL_PATH!r})
checks_path = batch_root / "scripts" / "checks"

sys.path.insert(0, str(checks_path))
sys.path.insert(0, str(batch_root))

from check_silver_output import check_silver_output

candidate_file = Path(
    {str(BATCH_SILVER_CANDIDATE_FILE)!r}
)

final_file = Path(
    {str(BATCH_SILVER_FINAL_FILE)!r}
)

result = check_silver_output(
    silver_file=candidate_file
)

print(
    f"Silver validation status: {{result['status']}}"
)

if result["status"] == "FAIL":
    raise RuntimeError(
        "Silver validation failed: "
        + "; ".join(result["critical_failures"])
    )

candidate_file.replace(final_file)

print(
    f"Silver candidate published to: {{final_file}}"
)
"""

    result = subprocess.run(
        [
            "python",
            "-c",
            command,
        ],
        cwd=BATCH_ETL_PATH,
        env={
            **os.environ,
            "PYTHONPATH": BATCH_ETL_PATH,
        },
        check=False,
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.returncode != 0:
        raise RuntimeError(
            "Silver validation task failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return {
        "validation_status": "passed",
        "stdout": result.stdout.strip(),
    }


def validate_gold_outputs() -> dict:
    command = f"""
import sys
from pathlib import Path

batch_root = Path({BATCH_ETL_PATH!r})
checks_path = batch_root / "scripts" / "checks"

sys.path.insert(0, str(checks_path))
sys.path.insert(0, str(batch_root))

from check_gold_outputs import (
    check_gold_outputs,
    publish_gold_candidate,
)

result = check_gold_outputs()

print(
    f"Gold validation status: {{result['status']}}"
)

if result["status"] == "FAIL":
    raise RuntimeError(
        "Gold validation failed. "
        f"Failed marts: {{result['failed_mart_count']}}"
    )

publish_gold_candidate()

print(
    "Gold candidate set published successfully."
)
"""

    result = subprocess.run(
        [
            "python",
            "-c",
            command,
        ],
        cwd=BATCH_ETL_PATH,
        env={
            **os.environ,
            "PYTHONPATH": BATCH_ETL_PATH,
        },
        check=False,
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.returncode != 0:
        raise RuntimeError(
            "Gold validation task failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return {
        "validation_status": "passed",
        "stdout": result.stdout.strip(),
    }


def run_build_gold_with_recovery(**context) -> dict:
    dag_run = context["dag_run"]

    pipeline_run_id = get_or_create_recovery_run(
        airflow_run_id=dag_run.run_id,
    )

    dataset_version_id = get_or_create_dataset_version(
        pipeline_run_id=pipeline_run_id,
        layer="GOLD",
    )

    update_dataset_version_status(
        dataset_version_id,
        "BUILDING",
    )

    artifact_ids = get_or_create_gold_artifacts(
        dataset_version_id=dataset_version_id,
    )

    artifact_execution_ids: dict[str, int] = {}

    for artifact_name, artifact_id in artifact_ids.items():
        artifact_execution_ids[artifact_name] = (
            start_recovery_artifact(
                artifact_id
            )
        )

    stage_execution_id = start_recovery_stage(
        pipeline_run_id=pipeline_run_id,
        stage_name="BUILD_GOLD",
        output_dataset_version_id=dataset_version_id,
    )

    result = subprocess.run(
        [
            "python",
            "scripts/pipeline/build_gold_marts.py",
        ],
        cwd=BATCH_ETL_PATH,
        env={
            **os.environ,
            "PYTHONPATH": BATCH_ETL_PATH,
        },
        check=False,
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.returncode != 0:
        error_message = (
            "BUILD_GOLD failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

        for artifact_execution_id in (
            artifact_execution_ids.values()
        ):
            finish_recovery_artifact(
                artifact_execution_id,
                False,
                error_message=error_message,
            )

        update_dataset_version_status(
            dataset_version_id,
            "FAILED",
        )

        finish_recovery_stage(
            stage_execution_id=stage_execution_id,
            success=False,
            error_message=error_message,
        )

        raise RuntimeError(error_message)

    for artifact_execution_id in (
        artifact_execution_ids.values()
    ):
        finish_recovery_artifact(
            artifact_execution_id,
            True,
        )

    update_dataset_version_status(
        dataset_version_id,
        "TRANSFORMED",
    )

    finish_recovery_stage(
        stage_execution_id=stage_execution_id,
        success=True,
    )

    return {
        "pipeline_run_id": pipeline_run_id,
        "dataset_version_id": dataset_version_id,
        "artifact_ids": artifact_ids,
        "artifact_execution_ids": artifact_execution_ids,
        "stage_execution_id": stage_execution_id,
        "stage_name": "BUILD_GOLD",
        "status": "SUCCEEDED",
    }


def run_check_gold_with_recovery(**context) -> dict:
    dag_run = context["dag_run"]

    pipeline_run_id = get_or_create_recovery_run(
        airflow_run_id=dag_run.run_id,
    )

    dataset_version_id = get_or_create_dataset_version(
        pipeline_run_id=pipeline_run_id,
        layer="GOLD",
    )

    stage_execution_id = start_recovery_stage(
        pipeline_run_id=pipeline_run_id,
        stage_name="CHECK_GOLD",
        input_dataset_version_id=dataset_version_id,
        output_dataset_version_id=dataset_version_id,
    )

    try:
        validation_result = validate_gold_outputs()

    except Exception as exc:
        update_dataset_version_status(
            dataset_version_id,
            "FAILED",
        )

        finish_recovery_stage(
            stage_execution_id=stage_execution_id,
            success=False,
            error_message=str(exc),
        )

        raise

    update_dataset_version_status(
        dataset_version_id,
        "PUBLISHED",
    )

    with get_recovery_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE recovery.dataset_artifacts
                SET status = 'PUBLISHED'
                WHERE dataset_version_id = %s;
                """,
                (dataset_version_id,),
            )

    finish_recovery_stage(
        stage_execution_id=stage_execution_id,
        success=True,
    )

    return {
        "pipeline_run_id": pipeline_run_id,
        "dataset_version_id": dataset_version_id,
        "stage_execution_id": stage_execution_id,
        "stage_name": "CHECK_GOLD",
        "status": "SUCCEEDED",
        "validation_result": validation_result,
    }


def run_upload_s3_with_recovery(**context) -> dict:
    return run_with_recovery(
        context=context,
        stage_name="UPLOAD_S3",
        operation=upload_batch_gold_to_s3,
    )


def run_redshift_create_schemas_with_recovery(
    **context,
) -> dict:
    return run_with_recovery(
        context=context,
        stage_name="REDSHIFT_CREATE_SCHEMAS",
        operation=redshift_create_schemas,
    )


def run_redshift_create_batch_landing_tables_with_recovery(
    **context,
) -> dict:
    return run_with_recovery(
        context=context,
        stage_name="REDSHIFT_CREATE_BATCH_LANDING_TABLES",
        operation=redshift_create_batch_landing_tables,
    )


def run_redshift_copy_batch_gold_with_recovery(
    **context,
) -> dict:
    return run_with_recovery(
        context=context,
        stage_name="REDSHIFT_COPY_BATCH_GOLD",
        operation=redshift_copy_batch_gold_from_s3,
    )


def run_redshift_create_analytics_views_with_recovery(
    **context,
) -> dict:
    return run_with_recovery(
        context=context,
        stage_name="REDSHIFT_CREATE_ANALYTICS_VIEWS",
        operation=redshift_create_batch_analytics_views,
    )


def run_redshift_validate_analytics_with_recovery(
    **context,
) -> dict:
    return run_with_recovery(
        context=context,
        stage_name="REDSHIFT_VALIDATE_ANALYTICS",
        operation=redshift_validate_batch_analytics,
    )


def run_cross_layer_validation_with_recovery(
    **context,
) -> dict:
    return run_with_recovery(
        context=context,
        stage_name="CROSS_LAYER_VALIDATION",
        operation=run_batch_cross_layer_validation,
    )


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
            "VENDOR_BATCH_ETL_CONTAINER_PATH": BATCH_ETL_PATH,
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
        failure_message="Redshift schema creation failed.",
        status_key="redshift_schema_creation_status",
    )


def redshift_create_batch_landing_tables() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_BATCH_TABLES_SQL,
        failure_message=(
            "Redshift Batch landing table creation failed."
        ),
        status_key=(
            "redshift_batch_landing_table_creation_status"
        ),
    )


def redshift_copy_batch_gold_from_s3() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_COPY_BATCH_GOLD_SQL,
        failure_message=(
            "Redshift Batch Gold COPY from S3 failed."
        ),
        status_key="redshift_batch_gold_copy_status",
    )


def redshift_create_batch_analytics_views() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_CREATE_BATCH_ANALYTICS_VIEWS_SQL,
        failure_message=(
            "Redshift Batch analytics view creation failed."
        ),
        status_key=(
            "redshift_batch_analytics_view_creation_status"
        ),
    )


def redshift_validate_batch_analytics() -> dict:
    return run_redshift_sql_task(
        sql_file=REDSHIFT_VALIDATE_BATCH_ANALYTICS_SQL,
        failure_message=(
            "Redshift Batch analytics validation query failed."
        ),
        status_key=(
            "redshift_batch_analytics_validation_status"
        ),
    )


def run_batch_cross_layer_validation() -> None:
    result = subprocess.run(
        [
            "python",
            "-m",
            "scripts.validation.run_batch_cross_layer_validation",
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
            "Batch cross-layer validation failed.\n"
            f"RETURN CODE: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    print(result.stdout)


with DAG(
    dag_id="vendor_payments_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["vendor-payments", "etl", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_batch_etl_scripts = BashOperator(
        task_id="check_batch_etl_scripts",
        bash_command=(
            f"test -s {BATCH_TRANSFORM_SILVER_SCRIPT} && "
            f"test -s {BATCH_CHECK_SILVER_SCRIPT} && "
            f"test -s {BATCH_BUILD_GOLD_SCRIPT} && "
            f"test -s {BATCH_CHECK_GOLD_SCRIPT}"
        ),
    )

    transform_silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=run_transform_silver_with_recovery,
    )

    check_silver_task = PythonOperator(
        task_id="check_silver",
        python_callable=run_check_silver_with_recovery,
    )

    build_gold_task = PythonOperator(
        task_id="build_gold",
        python_callable=run_build_gold_with_recovery,
    )

    check_gold_task = PythonOperator(
        task_id="check_gold",
        python_callable=run_check_gold_with_recovery,
    )

    upload_batch_gold_to_s3_task = PythonOperator(
        task_id="upload_batch_gold_to_s3",
        python_callable=run_upload_s3_with_recovery,
    )

    redshift_create_schemas_task = PythonOperator(
        task_id="redshift_create_schemas",
        python_callable=run_redshift_create_schemas_with_recovery,
    )

    redshift_create_batch_landing_tables_task = PythonOperator(
        task_id="redshift_create_batch_landing_tables",
        python_callable=(
            run_redshift_create_batch_landing_tables_with_recovery
        ),
    )

    redshift_copy_batch_gold_from_s3_task = PythonOperator(
        task_id="redshift_copy_batch_gold_from_s3",
        python_callable=run_redshift_copy_batch_gold_with_recovery,
    )

    redshift_create_batch_analytics_views_task = PythonOperator(
        task_id="redshift_create_batch_analytics_views",
        python_callable=(
            run_redshift_create_analytics_views_with_recovery
        ),
    )

    redshift_validate_batch_analytics_task = PythonOperator(
        task_id="redshift_validate_batch_analytics",
        python_callable=(
            run_redshift_validate_analytics_with_recovery
        ),
    )

    batch_cross_layer_validation_task = PythonOperator(
        task_id="validate_batch_cross_layer",
        python_callable=run_cross_layer_validation_with_recovery,
    )

    end = EmptyOperator(task_id="end")

    (
            start
            >> check_batch_etl_scripts
            >> transform_silver_task
            >> check_silver_task
            >> build_gold_task
            >> check_gold_task
            >> upload_batch_gold_to_s3_task
            >> redshift_create_schemas_task
            >> redshift_create_batch_landing_tables_task
            >> redshift_copy_batch_gold_from_s3_task
            >> redshift_create_batch_analytics_views_task
            >> redshift_validate_batch_analytics_task
            >> batch_cross_layer_validation_task
            >> end
    )
