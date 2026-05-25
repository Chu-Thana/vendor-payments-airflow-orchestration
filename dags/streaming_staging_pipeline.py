from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.notify import task_fail_alert, notify_success
from scripts.staging.extract_staging_sales import extract_staging_sales
from scripts.staging.transform_staging_sales import transform_staging_sales
from scripts.staging.load_staging_sales_summary import load_staging_sales_summary

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": task_fail_alert,
}

with DAG(
    dag_id="streaming_staging_pipeline",
    default_args=default_args,
    description="Read Kafka consumer staging file and build warehouse-ready summary",
    start_date=datetime(2026, 4, 14),
    schedule=None,
    catchup=False,
    tags=["streaming", "staging", "warehouse"],
    on_success_callback=notify_success,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_staging_sales",
        python_callable=extract_staging_sales,
    )

    transform_task = PythonOperator(
        task_id="transform_staging_sales",
        python_callable=transform_staging_sales,
    )

    load_task = PythonOperator(
        task_id="load_staging_sales_summary",
        python_callable=load_staging_sales_summary,
    )

    extract_task >> transform_task >> load_task