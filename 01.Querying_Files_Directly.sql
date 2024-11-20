-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').replace('@', '_').replace('.', '_')
-- MAGIC username

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("/Volumes/mehdidatalake_catalog/demo_bootcamp/landing_volume/gbfs/ebikes_at_station/")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Query a Single File
-- MAGIC
-- MAGIC To query the data contained in a single file, execute the query with the following pattern:
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC
-- MAGIC Make special note of the use of back-ticks (not single quotes) around the path. 

-- COMMAND ----------

SELECT * FROM json.`/Volumes/<USERNAME>/hol_schema/landing_volume/ebikes_at_station/<FILE_NAME>`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Query a Directory of Files
-- MAGIC
-- MAGIC Assuming all of the files in a directory have the same format and schema, all files can be queried simultaneously by specifying the directory path rather than an individual file.

-- COMMAND ----------

SELECT * FROM text.`/Volumes/<USERNAME>/hol_schema/landing_volume/ebikes_at_station/` limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC As long as a user has permission to access the view and the underlying storage location, that user will be able to use this view definition to query the underlying data. This applies to different users in the workspace, different notebooks, and different clusters.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Create References to Files
-- MAGIC This ability to directly query files and directories means that additional Spark logic can be chained to queries against files.
-- MAGIC
-- MAGIC When we create a view from a query against a path, we can reference this view in later queries.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ebikes_at_station_json_vw
AS SELECT * FROM json.`/Volumes/<USERNAME>/hol_schema/landing_volume/ebikes_at_station/<FILE_NAME>`
