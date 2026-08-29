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

BATCH_ETL_PATH = os.getenv(
    "VENDOR_BATCH_ETL_CONTAINER_PATH",
    "/opt/airflow/vendor_payments_batch_etl",
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


with DAG(
    dag_id="vendor_payments_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["vendor-payments", "etl", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_batch_etl_source = BashOperator(
        task_id="check_batch_etl_source",
        bash_command=(
            f"test -s "
            f"{BATCH_ETL_PATH}/scripts/pipeline/run_pipeline.py"
        ),
    )

    run_vendor_payments_pipeline = BashOperator(
        task_id="run_vendor_payments_pipeline",
        bash_command=(
            f"cd {BATCH_ETL_PATH} && "
            f"PYTHONPATH={BATCH_ETL_PATH} "
            "python scripts/pipeline/run_pipeline.py"
        ),
    )

    check_silver_output = BashOperator(
        task_id="check_silver_output",
        bash_command=(
            f"test -s "
            f"{BATCH_ETL_PATH}/data/processed/silver/"
            "vendor_payments_silver.csv "
            '&& echo "Silver output validation passed"'
        ),
    )

    check_gold_outputs = BashOperator(
        task_id="check_gold_outputs",
        bash_command=(
            f"test -s "
            f"{BATCH_ETL_PATH}/data/processed/gold_sample/"
            "mart_spending_by_fiscal_year.csv && "
            f"test -s "
            f"{BATCH_ETL_PATH}/data/processed/gold_sample/"
            "mart_spending_by_department.csv && "
            f"test -s "
            f"{BATCH_ETL_PATH}/data/processed/gold_sample/"
            "mart_spending_by_supplier_top_n.csv && "
            f"test -s "
            f"{BATCH_ETL_PATH}/data/processed/gold_sample/"
            "mart_pending_by_department.csv && "
            f"test -s "
            f"{BATCH_ETL_PATH}/data/processed/gold_sample/"
            "mart_fund_category_summary.csv"
        ),
    )

    upload_batch_gold_to_s3_task = PythonOperator(
        task_id="upload_batch_gold_to_s3",
        python_callable=upload_batch_gold_to_s3,
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

    end = EmptyOperator(task_id="end")

    (
            start
            >> check_batch_etl_source
            >> run_vendor_payments_pipeline
            >> check_silver_output
            >> check_gold_outputs
            >> upload_batch_gold_to_s3_task
            >> redshift_create_schemas_task
            >> redshift_create_batch_landing_tables_task
            >> redshift_copy_batch_gold_from_s3_task
            >> redshift_create_batch_analytics_views_task
            >> redshift_validate_batch_analytics_task
            >> end
    )