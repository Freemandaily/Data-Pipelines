from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ethereum_elt_pipeline',
    default_args=default_args,
    description='Orchestrate Extract, Token Fetch, and dbt modeling',
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ethereum', 'elt'],
) as dag:

    # Task 1: Extract data from Subsquid to Postgres
    # Assuming the Python script manages the watermark state in the DB
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command='cd /opt/airflow/src && python extract.py',
    )

    # Task 2: Fetch Token Metadata
    # Uses the RPC_URL from the Airflow environment
    fetch_metadata = BashOperator(
        task_id='fetch_metadata',
        bash_command='cd /opt/airflow/src && python fetch_token_metadata.py --rpc $RPC_URL',
    )

    # Task 3: Run dbt models
    # Uses profiles.yml located in the dbt_model directory
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_model && dbt run --profiles-dir .',
    )

    extract_data >> fetch_metadata >> run_dbt_models
