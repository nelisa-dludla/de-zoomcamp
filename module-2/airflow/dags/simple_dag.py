from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'nelisa',
    'start_date': datetime(2024, 7, 19),
    'schedule_interval': None
}

dag = DAG(dag_id='simple_dag', default_args=default_args)

def taskOne():
    print('Running task one...')

def taskTwo():
    print('Running task two...')

task1 = PythonOperator(
    task_id= 'task_one',
    python_callable=taskOne,
    dag=dag
)

task2 = PythonOperator(
    task_id='task_two',
    python_callable=taskTwo,
    dag=dag
)

task1 >> task2
