from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'nelisa',
    'start_date': datetime(2024, 7, 19),
    'schedule_interval': None
}

dag = DAG(
    dag_id='add_table_data_postgres',
    default_args=default_args
)

create_table = PostgresOperator(
    task_id='create_table',
    dag=dag,
    postgres_conn_id='postgres_zoomcamp',
    sql='''
        CREATE TABLE IF NOT EXISTS anime (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    '''
)

insert_data = PostgresOperator(
    task_id='insert_data',
    dag=dag,
    postgres_conn_id='postgres_zoomcamp',
    sql='''
        INSERT INTO anime (name) VALUES ('One Piece'), ('Naruto'), ('Bleach');
    '''
)

print_message = BashOperator(
    task_id='print_message',
    dag=dag,
    bash_command='echo "Data loaded successfully."'
)

create_table >> insert_data >> print_message
