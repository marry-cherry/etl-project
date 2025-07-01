from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'marina'
}

with DAG(
    dag_id='etl_md_ledger_account_s',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    run_etl = BashOperator(
        task_id='run_md_ledger_account_s',
        bash_command='cd /home/marinaub/PycharmProjects/etl_project && .venv/bin/python etl.py md_ledger_account_s'
    )

