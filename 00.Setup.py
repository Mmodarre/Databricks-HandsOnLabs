# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').replace('@', '_').replace('.', '_')
username

# COMMAND ----------

# DBTITLE 1,Create a catalog Creation
spark.sql(f"CREATE CATALOG IF NOT EXISTS {username}")

# COMMAND ----------

# DBTITLE 1,Creating a Schema in Spark SQL Using Python
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {username}.hol_schema")

# COMMAND ----------

# DBTITLE 1,Creating a New Volume in Spark SQL If Not Exists
spark.sql(f"CREATE volume IF NOT EXISTS {username}.hol_schema.landing_volume")

# COMMAND ----------

# DBTITLE 1,Retrieving and Saving Ebike Data Every Minute
import datetime, time
import requests

end_time = datetime.datetime.now() + datetime.timedelta(hours=1, minutes=0)

while datetime.datetime.now() < end_time:
    time_to_next_execution = (60 - datetime.datetime.now().second + 10) % 60
    time.sleep(time_to_next_execution)
    response = requests.get("https://gbfs.lyft.com/gbfs/1.1/bos/en/ebikes_at_stations.json")
    current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"ebikes_at_station_{current_datetime}.json"
    with open(f"/Volumes/{username}/hol_schema/landing_volume/ebikes_at_station/{file_name}", "wb") as file:
        file.write(response.content)
    if (datetime.datetime.now() >= end_time):
        break
