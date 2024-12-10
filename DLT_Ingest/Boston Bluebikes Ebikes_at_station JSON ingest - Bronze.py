# Databricks notebook source
import dlt

# COMMAND ----------

# DBTITLE 1,Schema Extraction from JSON Data Using PySpark
df_schema = (
    spark.read.format("json")
    .load(
        "/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/gbfs/station_status/station_status_20241020111610.json"
    )
    .select("data.stations")
)
schema = df_schema.schema

# COMMAND ----------

# DBTITLE 1,Streaming ETL for eBike Data Using PySpark
from pyspark.sql.functions import explode, from_json, col


@dlt.table(
    cluster_by = ["station_id"]
)
def station_status_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(
            "/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/gbfs/station_status/"
        )
        .withColumn("data", from_json(col("data"), schema))
        .withColumn("station", explode(col("data.stations")))
        .select("last_updated", "station.*")
    )