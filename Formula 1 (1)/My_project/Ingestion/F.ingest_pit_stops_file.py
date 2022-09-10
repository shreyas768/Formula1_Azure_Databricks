# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest pit_stops JSON file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

schema_pit_schema = StructType(fields=[StructField("raceId", IntegerType(), False), StructField("driverId", IntegerType(), True),StructField("stop", StringType(), True),StructField("lap", IntegerType(), True),StructField("time", StringType(), True), StructField("duration", StringType(), True),StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pit_stops = spark.read.schema(schema_pit_schema).option("multiLine", True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

pit_stops = add_ingestion_date(pit_stops)

# COMMAND ----------

display(pit_stops)

# COMMAND ----------

pit_stops_final = pit_stops.selectExpr("raceId as race_id", "driverId as driver_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(pit_stops_final)

# COMMAND ----------

#pit_stops_final.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

#pit_stops_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed_new.pit_stops")

# COMMAND ----------

#overwrite_partition(pit_stops_final, 'f1_processed_new', 'pit_stops', 'race_id')

# COMMAND ----------

merge_cond = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final, 'f1_processed_dl', 'pit_stops', processeddl_folder_path, merge_cond, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


