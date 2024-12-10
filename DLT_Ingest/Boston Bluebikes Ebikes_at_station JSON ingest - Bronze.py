# Databricks notebook source
import dlt
# COMMAND ----------

catalog = spark.conf.get("catalog")
# COMMAND ----------

# DBTITLE 1,Schema Extraction from JSON Data Using PySpark
df_schema = spark.read.format("json").load(f"/Volumes/{catalog}/hol_schema/landing_volume/ebikes_at_station/ebikes_at_station_XXXXXXXX.json").select("data.stations")
schema = df_schema.schema

# COMMAND ----------

# DBTITLE 1,Streaming ETL for eBike Data Using PySpark
from pyspark.sql.functions import explode, from_json, col


@dlt.table(
    cluster_by = ["station_id"]
)
def ebikes_at_station_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("inferSchema", "true")
        .load(
            f"/Volumes/{catalog}/hol_schema/landing_volume/ebikes_at_station/"
        )
        .withColumn("data", from_json(col("data"), schema))
        .withColumn("station", explode(col("data.stations")))
        .select("last_updated", "station.*")
        .withColumn("ebike", explode(col("ebikes")))
        .select("last_updated", "station_id", "ebike.*", "ebike.range_estimate.*")
        .drop("range_estimate")
    )