"""
Airflow DAG wrapping the same pipeline stages as src/pipeline.py, but with
proper scheduling, retries, and per-task observability instead of a linear
Python script.

This DAG intentionally reuses ingest.py / transform_validate.py as-is --
the business logic doesn't change when you move orchestrators, only how
it's scheduled, retried, and monitored. That's the actual argument for
using Airflow over a cron'd script: task-level retry/alerting, not
different transform code.

Task graph:

    ingest_batch --\
                     >--> transform_and_validate
    ingest_incremental --/

Both ingestion tasks can run in parallel since they write to different
staging tables; transform_and_validate depends on both completing.

To run for real: `pip install apache-airflow`, place this file (and the
src/ package on PYTHONPATH) under Airflow's dags/ folder, then
`airflow dags trigger ecom_pipeline`.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import ingest
import transform_validate

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="ecom_pipeline",
    description="Batch + incremental order ingestion, harmonization, and validation",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-engineering", "etl"],
) as dag:

    ingest_batch = PythonOperator(
        task_id="ingest_batch",
        python_callable=ingest.load_batch,
    )

    ingest_incremental = PythonOperator(
        task_id="ingest_incremental",
        python_callable=ingest.load_incremental,
    )

    transform_and_validate = PythonOperator(
        task_id="transform_and_validate",
        python_callable=transform_validate.run,
    )

    [ingest_batch, ingest_incremental] >> transform_and_validate
