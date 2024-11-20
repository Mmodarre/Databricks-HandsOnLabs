-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 1,Create a Temporary view on JSON file(s)
CREATE OR REPLACE TEMPORARY VIEW ebikes_at_station_json_vw
AS SELECT * FROM json.`/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/gbfs/ebikes_at_station/ebikes_at_station_20241020111711.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Flatten JSON data
-- MAGIC
-- MAGIC Here we use SQL to retrieves data from a JSON view, explodes nested arrays to rows, and excludes certain fields
-- MAGIC
-- MAGIC ###Later views
-- MAGIC
-- MAGIC In SQL, a LATERAL VIEW is used to apply a table-generating function to each row of a base table and then join the output rows with the base table. This is particularly useful when dealing with complex data types like arrays or maps, allowing you to explode these types into individual rows.
-- MAGIC
-- MAGIC `SELECT 
-- MAGIC   base_table.id,
-- MAGIC   exploded_table.value
-- MAGIC FROM 
-- MAGIC   base_table
-- MAGIC LATERAL VIEW explode(base_table.array_column) exploded_table AS value`
-- MAGIC
-- MAGIC In this example:
-- MAGIC
-- MAGIC base_table is the original table.
-- MAGIC explode is a table-generating function that takes an array and returns a set of rows, one for each element in the array.
-- MAGIC exploded_table is the alias for the result of the explode function.
-- MAGIC value is the alias for the elements of the array.
-- MAGIC This allows you to work with each element of the array as if it were a separate row in the table.
-- MAGIC

-- COMMAND ----------

-- This SQL query is designed to select and display information about ebikes at stations
-- It retrieves data from a JSON view, explodes nested arrays to rows, and excludes certain fields
SELECT
  station.* -- Selects all fields from the 'station' exploded view except for the 'ebikes' field
EXCEPT
  (ebikes),
  ebikes.* -- Selects all fields from the 'ebikes' exploded view except for the 'range_estimate' field
EXCEPT
  (range_estimate),
  ebikes.range_estimate.conservative_range_miles, -- Selects the 'conservative_range_miles' from the nested 'range_estimate' field
  ebikes.range_estimate.estimated_range_miles, -- Selects the 'estimated_range_miles' from the nested 'range_estimate' field
  last_updated -- Selects the 'last_updated' field to track the last update time
FROM
  ebikes_at_station_json_vw -- Specifies the source view containing JSON data
LATERAL VIEW EXPLODE(data.stations) AS station -- Explodes the 'stations' array into rows, creating a new row for each element in the array
LATERAL VIEW EXPLODE(station.ebikes) AS ebikes -- Further explodes the 'ebikes' array within each station into rows, creating a new row for each ebike in the station
LIMIT
  10 -- Limits the result to the first 10 rows for quick inspection or testing

-- COMMAND ----------


-- Create or replace a temporary view named 'ebikes_at_station_vw'
create or replace temp view ebikes_at_station_vw as 
SELECT
  station.* -- Select all fields from the 'station' exploded view except for the 'ebikes' field
EXCEPT
  (ebikes),
  ebikes.* -- Select all fields from the 'ebikes' exploded view except for the 'range_estimate' field
EXCEPT
  (range_estimate),
  ebikes.range_estimate.conservative_range_miles, -- Select the 'conservative_range_miles' from the nested 'range_estimate' field
  ebikes.range_estimate.estimated_range_miles, -- Select the 'estimated_range_miles' from the nested 'range_estimate' field
  last_updated -- Select the 'last_updated' field to track the last update time
FROM
  ebikes_at_station_json_vw -- Specify the source view containing JSON data
LATERAL VIEW EXPLODE(data.stations) AS station -- Explode the 'stations' array into rows, creating a new row for each element in the array
LATERAL VIEW EXPLODE(station.ebikes) AS ebikes -- Further explode the 'ebikes' array within each station into rows, creating a new row for each ebike in the station

-- COMMAND ----------

select * from ebikes_at_station_vw

-- COMMAND ----------

CREATE TABLE mehdidatalake_catalog.demo_bootcamp.ebikes_at_station as 
select * FROM ebikes_at_station_vw

-- COMMAND ----------

-- select * from mehdidatalake_catalog.demo_bootcamp.ebikes_at_station

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Option 2: Processing the JSON as text file
-- MAGIC
-- MAGIC The following cells will create a temporary view from a JSON file and then retrieve its schema:
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Create Temp. View of one of the files
CREATE OR REPLACE TEMPORARY VIEW ebikes_at_station_text_vw
AS SELECT * FROM text.`/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/gbfs/ebikes_at_station/ebikes_at_station_20241020111711.json`

-- COMMAND ----------

-- DBTITLE 1,use`schema_of_json` to generate the JSON file schema
select schema_of_json(value) from ebikes_at_station_text_vw

-- COMMAND ----------

-- DBTITLE 1,Use `from_json` function to parse the JSON file
SELECT json.* FROM (
SELECT from_json(value, 'STRUCT<data: STRUCT<stations: ARRAY<STRUCT<ebikes: ARRAY<STRUCT<battery_charge_percentage: BIGINT, displayed_number: STRING, docking_capability: BIGINT, is_lbs_internal_rideable: BOOLEAN, make_and_model: STRING, range_estimate: STRUCT<conservative_range_miles: DOUBLE, estimated_range_miles: DOUBLE>, rideable_id: STRING>>, station_id: STRING>>>, last_updated: BIGINT, ttl: BIGINT, version: STRING>') AS json 
FROM ebikes_at_station_text_vw);

-- COMMAND ----------

-- DBTITLE 1,Flatten the nested JSON to rows
SELECT
  station.*
EXCEPT
  (ebikes),
  ebikes.*
EXCEPT
  (range_estimate),
  ebikes.range_estimate.conservative_range_miles,
  ebikes.range_estimate.estimated_range_miles,
  json.last_updated -- ,range_estimate.*
FROM
  (
    SELECT
      json.*
    FROM
      (
        SELECT
          from_json(
            value,
            'STRUCT<data: STRUCT<stations: ARRAY<STRUCT<ebikes: ARRAY<STRUCT<battery_charge_percentage: BIGINT, displayed_number: STRING, docking_capability: BIGINT, is_lbs_internal_rideable: BOOLEAN, make_and_model: STRING, range_estimate: STRUCT<conservative_range_miles: DOUBLE, estimated_range_miles: DOUBLE>, rideable_id: STRING>>, station_id: STRING>>>, last_updated: BIGINT, ttl: BIGINT, version: STRING>'
          ) AS json
        FROM
          ebikes_at_station_text_vw
      ) as json
  ) LATERAL VIEW EXPLODE(json.data.stations) AS station LATERAL VIEW EXPLODE(station.ebikes) AS ebikes
LIMIT
  10
