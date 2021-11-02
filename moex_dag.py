from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

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

piplines = {'moex_get_securities_db.py': {"schedule": "55 6 * * 1-5"},
            "moex_get_marketdata_db.py": {"schedule": "*/2 7-23 * * 1-5"}}

def init_dag(dag, task_id):
    with dag:
        t1 = BashOperator(
            task_id=f"{task_id}",
            bash_command=f'python3 /Users/miracl6/airflow-mlflow-tutorial/{task_id}.py')
    return dag

for task_id, params in piplines.items():
    # DAG - ациклический граф
    dag = DAG(task_id,
              schedule_interval=params['schedule'],
              max_active_runs=1,
              default_args=default_args
              )
    init_dag(dag, task_id)
    globals()[task_id] = dag