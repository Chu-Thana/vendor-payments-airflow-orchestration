from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT1_PATH = "/opt/airflow/vendor-payments-etl-analytics"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="vendor_payments_etl_orchestration",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["vendor-payments", "etl", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_project1_source = BashOperator(
        task_id="check_project1_source",
        bash_command=f"test -f {PROJECT1_PATH}/scripts/pipeline/run_pipeline.py",
    )

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(
            f"rm -f {PROJECT1_PATH}/data/processed/silver/vendor_payments_silver_sample.csv && "
            f"rm -rf {PROJECT1_PATH}/data/processed/gold_sample && "
            f"mkdir -p {PROJECT1_PATH}/data/processed/gold_sample"
        ),
    )

    run_vendor_payments_pipeline = BashOperator(
        task_id="run_vendor_payments_pipeline",
        bash_command=(
            f"cd {PROJECT1_PATH} && "
            f"PYTHONPATH={PROJECT1_PATH} "
            "python scripts/pipeline/run_pipeline.py --sample"
        ),
    )

    check_silver_output = BashOperator(
        task_id="check_silver_output",
        bash_command=(
            f"test -f {PROJECT1_PATH}/data/processed/silver/vendor_payments_silver_sample.csv"
        ),
    )

    check_gold_outputs = BashOperator(
        task_id="check_gold_outputs",
        bash_command=(
            f"test -f {PROJECT1_PATH}/data/processed/gold_sample/mart_spending_by_fiscal_year.csv && "
            f"test -f {PROJECT1_PATH}/data/processed/gold_sample/mart_spending_by_department.csv && "
            f"test -f {PROJECT1_PATH}/data/processed/gold_sample/mart_spending_by_supplier_top_n.csv && "
            f"test -f {PROJECT1_PATH}/data/processed/gold_sample/mart_pending_by_department.csv && "
            f"test -f {PROJECT1_PATH}/data/processed/gold_sample/mart_fund_category_summary.csv"
        ),
    )

    end = EmptyOperator(task_id="end")

    (
            start
            >> check_project1_source
            >> clean_previous_outputs
            >> run_vendor_payments_pipeline
            >> check_silver_output
            >> check_gold_outputs
            >> end
    )