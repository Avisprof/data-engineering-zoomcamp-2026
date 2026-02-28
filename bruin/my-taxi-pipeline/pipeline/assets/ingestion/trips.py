"""@bruin
# ingestion.trips: fetch monthly parquet files from TLC and append to raw table
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: append
columns:
  - name: vendor_id
    type: string
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: passenger_count
    type: integer
  - name: trip_distance
    type: float
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: payment_type
    type: string
  - name: fare_amount
    type: float
  - name: extra
    type: float
  - name: mta_tax
    type: float
  - name: tip_amount
    type: float
  - name: tolls_amount
    type: float
  - name: improvement_surcharge
    type: float
@bruin"""

# standard libraries for env vars and JSON handling
import os
import json
from datetime import datetime

# third‑party libraries for HTTP and data processing
import pandas as pd


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    # pipeline variables come in as JSON string
    vars_json = os.environ.get("BRUIN_VARS", "{}")
    pipeline_vars = json.loads(vars_json)
    taxi_types = pipeline_vars.get("taxi_types", [])

    # date window variables (provided by Bruin runtime)
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    if not start_date or not end_date:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")

    # convert to datetime objects (date strings are YYYY-MM-DD)
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)
    print(f"fetching data for {taxi_types} from {start_date} to {end_date}")

    # fetch each parquet file, add lineage column, and collect DataFrames
    dfs = []
    current = start
    while current < end:
        year = current.year
        month = current.month
        for taxi in taxi_types:
            df = fetch_data(taxi, year, month)
            dfs.append(df)
        # advance to first of next month
        if month == 12:
            current = datetime(year + 1, 1, 1)
        else:
            current = datetime(year, month + 1, 1)

    print('combining dataframes...')
    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        print("final row count:", len(result))
        return result
    else:
        # return empty frame with no columns (Bruin handles schema from header)
        return pd.DataFrame()

def fetch_data(taxi, year, month):
  print(f'start fetching data {taxi} {year} {month}')
  url = f"https://raw.githubusercontent.com/Avisprof/data-engineering-zoomcamp-2026/main/bruin/{taxi}_tripdata_{year}-{month:02d}.parquet"
  #url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month:02d}.parquet"
  print('read parquet from url:', url)
  df = pd.read_parquet(url)
  print(f"fetched {len(df)} rows from {url}")
  
  # standardize column names: yellow has tpep_ prefix, green has lpep_ prefix
  # rename to common names declared in the asset metadata
  rename_map = {
      'tpep_pickup_datetime': 'pickup_datetime',
      'tpep_dropoff_datetime': 'dropoff_datetime',
      'lpep_pickup_datetime': 'pickup_datetime',
      'lpep_dropoff_datetime': 'dropoff_datetime',
      'PULocationID': 'pickup_location_id',
      'DOLocationID': 'dropoff_location_id',
  }
  df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}) 
  df['taxi_type'] = taxi  # add column to distinguish yellow vs green
  return df