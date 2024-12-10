-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW station_information_VW AS
SELECT
  to_timestamp(CAST(last_updated AS LONG)) timestamp_utc,
  to_utc_timestamp(
    (from_unixtime(CAST(last_updated AS LONG))),
    'UTC+4'
  ) timestamp_local,
  CAST(last_updated AS LONG) as last_updated,
  *
EXCEPT(last_updated)
FROM
  STREAM(Live.station_information_bronze)

-- COMMAND ----------

USE CATALOG '${catalog}';
USE SCHEMA hol_schema;
CREATE OR REFRESH STREAMING TABLE
  station_information_scd2_silver CLUSTER BY (station_id, `__END_AT`)

-- COMMAND ----------

USE CATALOG '${catalog}';
USE SCHEMA hol_schema;
APPLY CHANGES INTO station_information_scd2_silver
FROM
  STREAM(LIVE.station_information_VW) KEYS (station_id) SEQUENCE BY last_updated STORED AS SCD TYPE 2 TRACK HISTORY ON *
EXCEPT
  (timestamp_utc,timestamp_local,last_updated)

-- COMMAND ----------

USE CATALOG '${catalog}';
USE SCHEMA hol_schema;
CREATE MATERIALIZED VIEW
  station_information_latest_gold AS
select
  concat('motivate_BOS_', station_id) as station_id,
  *
EXCEPT
  (station_id, `__START_AT`, `__END_AT`)
from
  mehdidatalake_catalog.edw_bluebiks_ebikes_silver.station_information_scd2_silver
where
  `__END_AT` is null