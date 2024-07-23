#!/usr/bin/env python
import pandas as pd
import requests
from urllib.request import urlretrieve


# Fetch data
url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
filename = 'yellow_tripdata_2021-01.csv.gz'

urlretrieve(url, filename)

taxi_dtypes = {
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

parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
df = pd.read_csv(filename, compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)

zero_passenger_trips = df.loc[df.passenger_count == 0]
print('Number of trips with 0 passengers: ', len(zero_passenger_trips))

# Transform
valid_passenger_trips = df.loc[df.passenger_count != 0]
display(valid_passenger_trips)
