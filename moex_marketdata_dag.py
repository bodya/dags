from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'bodya',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 1),
    'retries': 2,
    'catchup': False
}

dag = DAG('moex_marketdata_dag_name',
    default_args=default_args,
    schedule_interval='*/2 7-23 * * 1-5')

t1 = BashOperator(task_id='moex_marketdata_to_db_id', bash_command='python3 ~/airflow/dags/moex_marketdata_db.py', dag=dag)

t1
