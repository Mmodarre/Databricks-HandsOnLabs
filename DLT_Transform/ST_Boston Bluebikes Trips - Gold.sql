-- Databricks notebook source
CREATE TEMPORARY LIVE VIEW ebikes_trips_VW_1 AS
SELECT
  ebike_time_id,
  rideable_id,
  timestamp_utc update_time,
  LEAD(timestamp_utc) OVER (
    PARTITION BY rideable_id
    ORDER BY
      timestamp_utc
  ) AS next_update_time
FROM
  '${catalog}'.edw_bluebiks_ebikes_silver.ebikes_at_station_silver

-- COMMAND ----------

USE CATALOG '${catalog}';
USE SCHEMA hol_schema;
CREATE
OR REFRESH STREAMING TABLE ebikes_trips_st cluster by (trip_start_time_local, pickup_station_id) AS
select
  md5(concat(bs.ebike_time_id, bs2.ebike_time_id)) as trip_id,
  bs.ebike_time_id trip_start_id,
  bs2.ebike_time_id trip_end_id,
  bs.rideable_id,
  bs.timestamp_local as trip_start_time_local,
  bs2.timestamp_local as trip_end_time_local,
  (
    UNIX_TIMESTAMP(tt.next_update_time) - UNIX_TIMESTAMP(tt.update_time)
  ) as trip_duration_seconds,
  bs.station_id as pickup_station_id,
  bs2.station_id as dropoff_station_id,
  CASE
    WHEN UNIX_TIMESTAMP(tt.next_update_time) - UNIX_TIMESTAMP(tt.update_time) < 600 THEN '0-10 minutes'
    WHEN UNIX_TIMESTAMP(tt.next_update_time) - UNIX_TIMESTAMP(tt.update_time) < 1800 THEN '10-30 minutes'
    WHEN UNIX_TIMESTAMP(tt.next_update_time) - UNIX_TIMESTAMP(tt.update_time) < 3600 THEN '30-60 minutes'
    ELSE '60+ minutes'
  END AS trip_duration_bucket,
  bs.battery_charge_percentage as pickup_battery_charge_percentage,
  bs2.battery_charge_percentage as dropoff_battery_charge_percentage,
  bs.estimated_range_miles as pickup_estimated_range_miles,
  bs2.estimated_range_miles as dropoff_estimated_range_miles,
  case
    when bs.station_id = bs2.station_id Then TRUE
    ELSE FALSE
  END AS Single_station_trip,
  case
    when bs.station_id != bs2.station_id
    AND (
      bs2.battery_charge_percentage between bs.battery_charge_percentage
      and bs.battery_charge_percentage + 9.9
      or bs2.estimated_range_miles between bs.estimated_range_miles
      and bs.estimated_range_miles + 3.49
    ) Then TRUE
    ELSE FALSE
  END AS Rebalancing_trip,
  CASE
    WHEN bs2.battery_charge_percentage >= bs.battery_charge_percentage + 10
    or bs2.estimated_range_miles >= bs.estimated_range_miles + 3.5 THEN TRUE
    ELSE FALSE
  END AS maintenance_trip
from
  Live.ebikes_trips_VW_1 tt
  join '${catalog}'.hol_schema.ebikes_at_station_silver AS bs on tt.rideable_id = bs.rideable_id
  and tt.update_time = CAST(CAST(bs.last_updated AS BIGINT) AS TIMESTAMP)
  join STREAM(
    '${catalog}'.hol_schema.ebikes_at_station_silver
  ) bs2 on tt.rideable_id = bs2.rideable_id
  and tt.next_update_time = CAST(CAST(bs2.last_updated AS BIGINT) AS TIMESTAMP)
  and (
    bs2.station_id != bs.station_id
    OR bs2.estimated_range_miles < bs.estimated_range_miles
  )
where
  UNIX_TIMESTAMP(tt.next_update_time) - UNIX_TIMESTAMP(tt.update_time) > 60