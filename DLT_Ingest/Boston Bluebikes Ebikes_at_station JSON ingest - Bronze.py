# Databricks notebook source
import dlt
# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').replace('@', '_').replace('.', '_')
username
# COMMAND ----------

# DBTITLE 1,Schema Extraction from JSON Data Using PySpark
df_schema = (
    spark.read.format("json")
    .load(
        f"/Volumes/{username}/hol_schema/landing_volume/ebikes_at_station/station_status_XXXXX.json"
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
            f"/Volumes/{username}/hol_schema/landing_volume/ebikes_at_station/"
        )
        .withColumn("data", from_json(col("data"), schema))
        .withColumn("station", explode(col("data.stations")))
        .select("last_updated", "station.*")
    )
