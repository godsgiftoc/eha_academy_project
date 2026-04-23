"""
sales_pipeline DAG — orchestration only.

All business logic lives in include/sales_pipeline/tasks.py. Keeping this
file tiny means Airflow's DAG parser cycles fast.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Importable because docker-compose sets PYTHONPATH=/opt/airflow/include.
from sales_pipeline.tasks import (
    aggregate_gold,
    generate_report,
    ingest_bronze,
    on_failure,
    transform_silver,
)

default_args = {
    "owner": "eha-academy",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="sales_pipeline",
    description="Bronze -> Silver -> Gold -> Report for sales data",
    start_date=datetime(2026, 4, 1),
    schedule_interval="0 8 * * *",  # every day at 08:00 UTC
    catchup=False,                   # don't backfill historical runs
    default_args=default_args,
    tags=["eha", "demo", "medallion"],
) as dag:
    ingest = PythonOperator(task_id="ingest_bronze", python_callable=ingest_bronze)
    transform = PythonOperator(task_id="transform_silver", python_callable=transform_silver)
    aggregate = PythonOperator(task_id="aggregate_gold", python_callable=aggregate_gold)
    report = PythonOperator(
        task_id="generate_report",
        # Pass Airflow's logical date (`ds`) into the pure function.
        python_callable=lambda **ctx: generate_report(ctx["ds"]),
    )

    ingest >> transform >> aggregate >> report
