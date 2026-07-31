from __future__ import annotations
from datetime import datetime, timedelta
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

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": task_fail_alert,
}

with DAG(
    dag_id="vendor_payments_streaming_validation",
    default_args=default_args,
    description=(
        "Validate Kafka consumer staging data and build "
        "a warehouse-ready vendor payments summary"
    ),
    start_date=datetime(2026, 4, 14),
    schedule=None,
    catchup=False,
    tags=[
        "vendor-payments",
        "streaming",
        "validation",
    ],
    on_success_callback=notify_success,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_vendor_payments_staging",
        python_callable=extract_vendor_payments_staging,
    )

    transform_task = PythonOperator(
        task_id="transform_vendor_payments_staging",
        python_callable=transform_vendor_payments_staging,
    )

    load_task = PythonOperator(
        task_id="load_vendor_payments_summary",
        python_callable=load_vendor_payments_summary,
    )

    extract_task >> transform_task >> load_task