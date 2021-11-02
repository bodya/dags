from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
#from datetime import datetime

default_args = {
    'owner': 'bodya',
    'depends_on_past': False,
    #'start_date': datetime(2021, 11, 2),
    'start_date': days_ago(0),
    'retries': 0,
    'catchup': False
    # "retry_delay": datetime.timedelta(minutes=5),  # дельта запуска при повторе 5 минут
    #"task_concurency": 1  # одновременно только 1 таск
}

dag = DAG('moex_marketdata_dag_name',
    default_args=default_args,
    schedule_interval='*/2 7-23 * * 1-5')

t1 = BashOperator(task_id='moex_marketdata_to_db_id', bash_command='python3 ~/airflow/dags/moex_marketdata_db.py', dag=dag)

t1
