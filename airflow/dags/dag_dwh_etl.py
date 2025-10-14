"""
=================================================
Airflow DAG - ETL1 (Kaggle → Staging → DWH)
Using BashOperator
=================================================
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "irhammaula_ario",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dwh_etl_pipeline",
    description="ETL1 pipeline for Agrofood CO2 emissions: Kaggle → Staging → DWH",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # run manually
    catchup=False,
    tags=["ETL1", "DWH", "Agrofood"],
) as dag:

    extract_task = BashOperator(
        task_id="extract_from_kaggle",
        bash_command="python3 /opt/airflow/scripts/dwh_extract.py",
    )

    transform_task = BashOperator(
        task_id="transform_to_dim_fact",
        bash_command="python3 /opt/airflow/scripts/dwh_transform.py",
    )

    load_task = BashOperator(
        task_id="load_to_staging_and_dwh",
        bash_command="python3 /opt/airflow/scripts/dwh_load.py",
    )

    extract_task >> transform_task >> load_task
