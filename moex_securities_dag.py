from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'bodya',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 02),
    'retries': 2,
    'catchup': False
}

dag = DAG('moex_securities_dag_name',
    default_args=default_args,
    schedule_interval='55 6 * * 1-5')

t1 = BashOperator(task_id='moex_securities_to_db_id', bash_command='python3 ~/airflow/dags/moex_securities_db.py', dag=dag)

t1
