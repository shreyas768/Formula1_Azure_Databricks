-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dlshreyas/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed_new CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed_new
LOCATION "/mnt/formula1dlshreyas/processedtbl";

-- COMMAND ----------

drop DATABASE IF EXISTS f1_processed_dl cascade;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed_dl
LOCATION "/mnt/formula1dlshreyas/processeddl";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/formula1dlshreyas/presentation";

-- COMMAND ----------


