from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'bodya',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 20),
    'retries': 0
}

dag = DAG('my_first_dag_name', 
    default_args=default_args,
    schedule_interval='* * * * *')
    
def hello():
    print('Hello, world!')   
    
def sum_init():
    return print(2+2)

t1 = PythonOperator(task_id='my_first_task_print_id', python_callable=hello,
                    dag=dag)
                    
t2 = PythonOperator(task_id='my_first_task_sum_id', python_callable=sum_init,
                    dag=dag)  
       
t3 = BashOperator(task_id='my_first_task_bash_id', bash_command='python3 print('Another hello world from same code!')',
                    dag=dag)      
t1 >> t2 >> t3                    
