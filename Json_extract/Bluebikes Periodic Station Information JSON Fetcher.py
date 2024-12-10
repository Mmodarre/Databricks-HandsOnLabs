# Databricks notebook source
dbutils.widgets.text("job_run_id","")

# COMMAND ----------

import datetime,time
import requests

job_run_id = dbutils.widgets.get("job_run_id")
response = requests.get ("https://gbfs.lyft.com/gbfs/1.1/bos/en/station_information.json")
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
file_name = f"station_information_{current_datetime}_{job_run_id}.json"
with open(f"/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/gbfs/station_information/{file_name}", "wb") as file:
    file.write(response. content)