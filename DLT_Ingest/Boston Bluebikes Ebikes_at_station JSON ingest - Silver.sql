-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW ebikes_at_station_VW AS
SELECT
  md5(concat(rideable_id, last_updated)) as ebike_time_id,
  rideable_id,
  station_id,
  displayed_number,
  make_and_model,
  battery_charge_percentage,
  conservative_range_miles,
  estimated_range_miles,
  to_timestamp(CAST(last_updated AS LONG)) timestamp_utc,
  to_utc_timestamp(
    (from_unixtime(CAST(last_updated AS LONG))),
    'UTC+4'
  ) timestamp_local,
  CAST(last_updated AS LONG) as last_updated
FROM
  STREAM(Live.ebikes_at_station_raw)

-- COMMAND ----------

--USE CATALOG '${catalog}';
--USE SCHEMA hol_schema;
CREATE STREAMING TABLE
  ebikes_at_station_silver CLUSTER BY (rideable_id, last_updated)

-- COMMAND ----------

--USE CATALOG '${catalog}';
--USE SCHEMA hol_schema;
APPLY CHANGES INTO LIVE.ebikes_at_station_silver
FROM STREAM(LIVE.ebikes_at_station_VW)
KEYS (rideable_id, last_updated)
SEQUENCE BY last_updated
STORED AS SCD TYPE 1
