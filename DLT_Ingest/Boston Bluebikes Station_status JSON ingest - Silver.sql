-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW station_status_VW AS
SELECT
  md5(concat(station_id, last_updated)) as station_time_id,
  to_timestamp(CAST(last_updated AS LONG)) timestamp_utc,
  to_utc_timestamp(
    (from_unixtime(CAST(last_updated AS LONG))),
    'UTC+4'
  ) timestamp_local,
  CAST(last_updated AS LONG) as last_updated,
  *
EXCEPT(last_updated)
FROM
  STREAM(Live.station_status_raw)

-- COMMAND ----------

USE CATALOG '${catalog}';
USE SCHEMA hol_schema;
CREATE STREAMING TABLE
  station_status_silver CLUSTER BY (station_id, last_updated)

-- COMMAND ----------
USE CATALOG '${catalog}';
USE SCHEMA hol_schema;
APPLY CHANGES INTO station_status_silver
FROM
  STREAM(LIVE.station_status_VW) KEYS (station_id, last_updated) SEQUENCE BY last_updated STORED AS SCD TYPE 1