#!/usr/bin/env python
import os
import pandas as pd
from pandas.io.common import gzip
from sqlalchemy import create_engine
from time import time
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name

    csv_name = 'output.csv.gz'
    # Download CSV
    os.system(f'wget {url} -O {csv_name}')
    # SQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # Read csv
    with gzip.open(csv_name, 'rt') as file:
        df_iter = pd.read_csv(file, iterator=True, chunksize=100000)

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


        while True:
            t_start = time()

            try:
                df = next(df_iter)
            except StopIteration:
                break

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()
            print('Inserted another chunk..., took %.3f second' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postges')
    # User
    parser.add_argument('--user', help='Username for postgres')
    # Password
    parser.add_argument('--password', help='Password for postgres')
    # Host
    parser.add_argument('--host', help='H for postgres')
    # Port
    parser.add_argument('--port', help='Port for postgres')
    # Database Name
    parser.add_argument('--db', help='Database name for postgres')
    # Table Name
    parser.add_argument('--table_name', help='Table name of where results will be written to')
    # Url of CSV
    parser.add_argument('--url', help='url of csv file')

    args = parser.parse_args()

    main(args)
