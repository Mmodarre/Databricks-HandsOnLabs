# Databricks notebook source
# MAGIC %md
# MAGIC **Prompt:** download and unzip this link https://s3.amazonaws.com/hubway-data/202409-bluebikes-tripdata.zip

# COMMAND ----------


import urllib.request
import zipfile
import os

# Define the URL and the local file path
url = "https://s3.amazonaws.com/hubway-data/202410-bluebikes-tripdata.zip"
local_zip_path = "/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/historical/202410-bluebikes-tripdata.zip"
extract_path = "/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/historical/202410-bluebikes-tripdata"

# Download the file
urllib.request.urlretrieve(url, local_zip_path)

# Create the extract path if it doesn't exist
os.makedirs(extract_path, exist_ok=True)

# Unzip the file
with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# List the extracted files
extracted_files = os.listdir(extract_path)
display(extracted_files)

# COMMAND ----------

file_path = "/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/historical/202410-bluebikes-tripdata/202410-bluebikes-tripdata.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("rideable_type") == "electric_bike").write.mode("overwrite").saveAsTable(
    "mehdidatalake_catalog.edw_bluebikes_ebikes_bronze.historical_trips"
)

# COMMAND ----------

# DBTITLE 1,Top Five Most Frequent Bluebike Trip Routes
# MAGIC %sql
# MAGIC select
# MAGIC   s0.name as pickup_station_name,
# MAGIC   s1.name as dropoff_station_name,
# MAGIC   count(t.trip_id) as trip_count
# MAGIC from
# MAGIC   mehdidatalake_catalog.edw_bluebikes_ebikes_gold.ebikes_trips_st t
# MAGIC   left join mehdidatalake_catalog.edw_bluebikes_ebikes_gold.station_information_latest_gold s0 on t.pickup_station_id = s0.station_id
# MAGIC   left join mehdidatalake_catalog.edw_bluebikes_ebikes_gold.station_information_latest_gold s1 on t.dropoff_station_id = s1.station_id
# MAGIC
# MAGIC   where Rebalancing_trip is false -- and Single_station_trip is false
# MAGIC     and maintenance_trip is false
# MAGIC group by
# MAGIC   s0.name,
# MAGIC   s1.name
# MAGIC order by
# MAGIC   trip_count desc
# MAGIC limit
# MAGIC   5

# COMMAND ----------

# MAGIC %sql
# MAGIC select start_station_name, end_station_name, count(*) as trip_count
# MAGIC from mehdidatalake_catalog.edw_bluebikes_ebikes_bronze.historical_trips
# MAGIC group by start_station_name, end_station_name
# MAGIC order by trip_count desc
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mehdidatalake_catalog.edw_bluebikes_ebikes_bronze.historical_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, 
# MAGIC        (unix_timestamp(ended_at) - unix_timestamp(started_at)) as trip_duration_seconds,
# MAGIC        case 
# MAGIC            when (unix_timestamp(ended_at) - unix_timestamp(started_at)) < 600 then '0-10 minutes'
# MAGIC            when (unix_timestamp(ended_at) - unix_timestamp(started_at)) < 1800 then '10-30 minutes'
# MAGIC            when (unix_timestamp(ended_at) - unix_timestamp(started_at)) < 3600 then '30-60 minutes'
# MAGIC            else '60+ minutes'
# MAGIC        end as trip_duration_bucket
# MAGIC from mehdidatalake_catalog.edw_bluebikes_ebikes_bronze.historical_trips
# MAGIC where date(started_at) between '2024-10-28' and '2024-10-31'

# COMMAND ----------

from pyspark.sql.functions import col, when
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Load the dataset
file_path_new = "/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/historical/202410-bluebikes-tripdata/202410-bluebikes-tripdata.csv"
df_new = spark.read.csv(file_path_new, header=True, inferSchema=True)

# Prepare the data
df = df.withColumn("label", when(col("member_casual") == "member", 1).otherwise(0))
indexer = StringIndexer(inputCols=["rideable_type", "start_station_name", "end_station_name"], outputCols=["rideable_type_index", "start_station_index", "end_station_index"])
assembler = VectorAssembler(inputCols=["rideable_type_index", "start_station_index", "end_station_index"], outputCol="features")

# Train the model
rf = RandomForestClassifier(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[indexer, assembler, rf])
model = pipeline.fit(df)

# Generate predictions for the new dataset
df_new_transformed = model.transform(df_new)
df_new_with_predictions = df_new_transformed.withColumn("member_casual", when(col("prediction") == 1, "member").otherwise("casual"))

display(df_new_with_predictions)