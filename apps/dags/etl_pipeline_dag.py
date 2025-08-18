from __future__ import annotations
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.adapters.logging import get_logger
from src.application.dq_and_load import load_and_prepare_all
from src.application.transform_service import build_output_and_export

log = get_logger()


def load_data_callable(**context):
    today = date.today().isoformat()
    log.info("run_start_load", today=today)
    try:
        dfs = load_and_prepare_all()
        context["ti"].xcom_push(key="dfs", value=dfs)
        log.info("load_done", today=today)
    except Exception:
        log.exception("load_failed", today=today)
        raise


def export_data_callable(**context):
    today = date.today().isoformat()
    log.info("run_start_export", today=today)
    try:
        dfs = context["ti"].xcom_pull(key="dfs", task_ids="load_data")
        out_dir = build_output_and_export(dfs)
        log.info("export_done", today=today, out_dir=out_dir)
    except Exception:
        log.exception("export_failed", today=today)
        raise


default_args = {
    "owner": "juan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="ETL pays/taps/prints",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl"],
) as dag:
    load_data = PythonOperator(
        task_id="load_data", python_callable=load_data_callable
    )
    export_data = PythonOperator(
        task_id="export_data", python_callable=export_data_callable
    )
    load_data >> export_data
