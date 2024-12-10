-- Databricks notebook source
-- create temporary STREAMING LIVE view station_information_latest_vw as
-- select * from mehdidatalake_catalog.edw_bluebiks_ebikes_silver.station_information_scd2_silver where `__end_at` is null

-- COMMAND ----------

USE CATALOG '${catalog}';
USE SCHEMA hol_schema;
CREATE
OR REFRESH MATERIALIZED VIEW ebikes_trips_stations_mv cluster by (trip_start_time_local) AS
SELECT
  t.*,
  s.name as pickup_station_name,
  s2.name as dropoff_station_name
FROM
  mehdidatalake_catalog.edw_bluebikes_ebikes_gold.ebikes_trips_st t
  left join mehdidatalake_catalog.edw_bluebiks_ebikes_silver.station_information_scd2_silver s on t.pickup_station_id = s.station_id and s.`__end_at` is null
  left join mehdidatalake_catalog.edw_bluebiks_ebikes_silver.station_information_scd2_silver s2 on t.dropoff_station_id = s2.station_id and s2.`__end_at` is null