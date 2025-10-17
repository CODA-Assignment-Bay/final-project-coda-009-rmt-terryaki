"""
DAG: dm_etl_pipeline
Purpose: Create Datamart in Neon PostgreSQL (schema = dm)
Author: SystemIQ Data Team
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ------------------------------------------------------------
# Default Args
# ------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------
with DAG(
    dag_id='dm_etl_pipeline',
    description='ETL 2 - Datamart creation from DWH in Neon PostgreSQL',
    default_args=default_args,
    start_date=datetime(2025, 10, 14),
    schedule_interval=None,  # manual trigger only
    catchup=False,
    tags=['datamart', 'neon', 'etl2'],
) as dag:

    # --------------------------------------------------------
    # 1️. Extract
    # --------------------------------------------------------
    extract_dm = BashOperator(
        task_id='dm_extract',
        bash_command='cd /opt/airflow/scripts && python3 dm_extract.py',
    )

    # --------------------------------------------------------
    # 2️. Transform
    # --------------------------------------------------------
    transform_dm = BashOperator(
        task_id='dm_transform',
        bash_command='cd /opt/airflow/scripts && python3 dm_transform.py',
    )

    # --------------------------------------------------------
    # 3️. Load
    # --------------------------------------------------------
    load_dm = BashOperator(
        task_id='dm_load',
        bash_command='cd /opt/airflow/scripts && python3 dm_load.py',
    )

    # --------------------------------------------------------
    # Dependencies
    # --------------------------------------------------------
    extract_dm >> transform_dm >> load_dm
