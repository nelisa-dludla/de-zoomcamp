from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
from urllib.request import urlretrieve
from sqlalchemy import create_engine

TAXI_DTYPES = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': pd.Float64Dtype(),
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': pd.StringDtype(),
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': pd.Float64Dtype(),
        'extra': pd.Float64Dtype(),
        'mta_tax': pd.Float64Dtype(),
        'tip_amount': pd.Float64Dtype(),
        'tolls_amount': pd.Float64Dtype(),
        'improvement_surcharge': pd.Float64Dtype(),
        'total_amount': pd.Float64Dtype(),
        'congestion_surcharge': pd.Float64Dtype()
    }

PARSE_DATES = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

default_args = {
    'owner': 'nelisa',
    'start_date': datetime(2024, 7, 21),
    'schedule_interval': None
}

dag = DAG(
    dag_id='api_to_postgres',
    default_args=default_args
)

def fetch_data():
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    filename = 'yellow_tripdata_2021-01.csv.gz'

    try:
        urlretrieve(url, filename)
        print(f'Successfully downloaded {filename}.')
    except Exception as error:
        print(f'Error downloading file: {error}')
        raise

def transform_data():
    filename = 'yellow_tripdata_2021-01.csv.gz'

    try:
        df = pd.read_csv(filename, compression='gzip', dtype=TAXI_DTYPES, parse_dates=PARSE_DATES)
        print(f'Successfully read {filename} into DataFrame.')
    except Exception as error:
        print(f'Error reading CSV file into DataFrame: {error}')
        raise

    # Number of trips with zero passengers
    zero_passenger_trips = df.loc[df.passenger_count == 0]
    print('Number of trips with 0 passengers: ', len(zero_passenger_trips))

    # Transform
    valid_passenger_trips = df.loc[df.passenger_count != 0]
    valid_passenger_trips.to_csv('cleaned_yellow_taxi_data.csv')


def load_data():
    filename = 'cleaned_yellow_taxi_data.csv'
    try:
        df = pd.read_csv(filename, dtype=TAXI_DTYPES, parse_dates=PARSE_DATES)
        print(f'Successfully read {filename} into DataFrame.')
    except Exception as error:
        print(f'Error reading CSV file into DataFrame: {error}')
        raise

    engine = create_engine('postgresql://root:root@postgres/postgres')
    output_table = 'yellow_taxi_data'
    df.to_sql(output_table, engine, index=False, if_exists='replace', chunksize=100000)


fetch = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

fetch >> transform >> load
