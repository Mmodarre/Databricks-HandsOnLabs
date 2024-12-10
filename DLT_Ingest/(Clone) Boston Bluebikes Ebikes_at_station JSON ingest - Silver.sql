-- Databricks notebook source
import requests
import pandas as pd
from pyspark.sql.functions import pandas_udf
from typing import Iterator, Tuple
from datetime import datetime, timedelta

import requests
import pandas as pd

def get_route(source_lat_series, source_lon_series, dest_lat_series, dest_lon_series):
    results = []
    
    # Iterate over each row in the series
    for source_lat, source_lon, dest_lat, dest_lon in zip(source_lat_series, source_lon_series, dest_lat_series, dest_lon_series):
        try:
            # Prepare coordinates and URL
            source_coordinates = f'{source_lon},{source_lat};'
            dest_coordinates = f'{dest_lon},{dest_lat}'
            url = 'http://router.project-osrm.org/route/v1/bike/' + source_coordinates + dest_coordinates

            # Define payload
            payload = {"steps": "true", "geometries": "geojson"}

            # Make API request
            response = requests.get(url, params=payload)
            response.raise_for_status()  # Check for request errors
            data = response.json()

            # Create DataFrame from response JSON
            df = pd.json_normalize(data['routes'][0]['legs'][0]['steps'])
            total_distance = round(df['distance'].sum() / 1000, 1)
            total_duration = round(df['duration'].sum(), 0)
            
            
            # Append formatted result to the list
            results.append(f'{total_distance};{total_duration}')
        
        except requests.exceptions.RequestException as e:
            results.append(f'Error: {str(e)}')
    
    # Convert the list of results to a Pandas Series and return
    return pd.Series(results)
    

@pandas_udf("string")
def get_route_pd(iterator: Iterator[Tuple[pd.Series, pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    for source_lat, source_lon, dest_lat, dest_lon in iterator:
        yield  get_route(source_lat, source_lon, dest_lat, dest_lon)

-- COMMAND ----------

spark.udf.register("get_route_pd", get_route_pd)

-- COMMAND ----------

CREATE TEMPORARY STREAMING LIVE VIEW ebikes_at_station_VW AS
with stations as (
  select
    distinct pickup_station_id,
    dropoff_station_id,
    s1.lat as pickup_station_lat,
    s1.lon as pickup_station_lon,
    s2.lat as dropoff_station_lat,
    s2.lon as dropoff_station_lon
  from
    mehdidatalake_catalog.edw_bluebikes_ebikes_gold.ebikes_trips_st ts
    join mehdidatalake_catalog.edw_bluebikes_ebikes_gold.station_information_latest_gold s1 on ts.pickup_station_id = s1.station_id
    join mehdidatalake_catalog.edw_bluebikes_ebikes_gold.station_information_latest_gold s2 on ts.dropoff_station_id = s2.station_id
    where ts.pickup_station_id != ts.dropoff_station_id
),
station_matrix as (
  select
    s.pickup_station_id source_station_id,
    s.pickup_station_lat source_station_lat,
    s.pickup_station_lon source_station_lon,
    s.dropoff_station_id destination_station_id,
    s.dropoff_station_lat destination_station_lat,
    s.dropoff_station_lon destination_station_lon --,get_route(s1.lat, s1.lon, s2.lat, s2.lon) as distance_duration
  from
    stations s ANTI
    JOIN mehdidatalake_catalog.edw_bluebikes_ebikes_gold.station_distance_matrix sdm on s.pickup_station_id = sdm.source_station_id
    and s.dropoff_station_id = sdm.destination_station_id
  limit
    4000
), distance_duration as (
  select
    *,
    get_route_pd(
      source_station_lat,
      source_station_lon,
      destination_station_lat,
      destination_station_lon
    ) as distance_duration
  from
    station_matrix
)
select
  source_station_id,
  destination_station_id,
  substring(
    distance_duration,
    1,
    instr(distance_duration, ';') -1
  ) as distance,
  substring(
    distance_duration,
    instr(distance_duration, ';') + 1
  ) as driving_travel_time
from
  distance_duration

-- COMMAND ----------

USE CATALOG mehdidatalake_catalog;
USE SCHEMA edw_bluebiks_ebikes_silver;
CREATE STREAMING TABLE
  ebikes_at_station_silver CLUSTER BY (rideable_id, last_updated)

-- COMMAND ----------

USE CATALOG mehdidatalake_catalog;
USE SCHEMA edw_bluebiks_ebikes_silver;
APPLY CHANGES INTO ebikes_at_station_silver
FROM STREAM(LIVE.ebikes_at_station_VW)
KEYS (rideable_id, last_updated)
SEQUENCE BY last_updated
STORED AS SCD TYPE 1
