from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'bodya',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 20),
    'retries': 0,
    'catchup': False
}

dag = DAG('my_first_dag_name',
    default_args=default_args,
    schedule_interval='*/1 * * * *')

def hello():
    print('Hello, world!')

def sum_init():
    return print(2+2)

t1 = PythonOperator(task_id='my_first_task_print_id', python_callable=hello,
                    dag=dag)

t2 = PythonOperator(task_id='my_first_task_sum_id', python_callable=sum_init,
                    dag=dag)

#t3 = BashOperator(task_id='my_first_task_bash_id', bash_command='echo "hello world form BASH!!!"', dag=dag)

t3 = BashOperator(task_id='my_first_task_bash_id', bash_command='python3 ~/airflow/dags/work_with_db.py', dag=dag)



t1 >> t2 >> t3
