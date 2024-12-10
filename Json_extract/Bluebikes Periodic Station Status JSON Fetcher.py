# Databricks notebook source
dbutils.widgets.text("job_run_id","")

# COMMAND ----------

import datetime,time
import requests

end_time = datetime.datetime.now() + datetime.timedelta(hours=14, minutes=0)
job_run_id = dbutils.widgets.get("job_run_id")
while datetime.datetime.now() < end_time:
    time_to_next_execution = (60 - datetime.datetime.now().second + 10) % 60
    time.sleep(time_to_next_execution)
    response = requests.get ("https://gbfs.lyft.com/gbfs/1.1/bos/en/station_status.json")
    current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"station_status_{current_datetime}_{job_run_id}.json"
    with open(f"/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/gbfs/station_status/{file_name}", "wb") as file:
        file.write(response. content)
    if (datetime.datetime.now() >= end_time):
        break