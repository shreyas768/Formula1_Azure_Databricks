# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying JSON folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp as ct, lit

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True), StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),])

# COMMAND ----------

qualifying = spark.read.schema(qualifying_schema).option("multiLine", True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying)

# COMMAND ----------

qualifying = add_ingestion_date(qualifying)

# COMMAND ----------

qualifying_final = qualifying.selectExpr("qualifyId as qualify_id","raceId as race_id","driverId as driver_id","constructorId as constructor_id").withColumn("ingestion_date", ct()).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

#qualifying_final.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

#display(spark.read.parquet('/mnt/formula1dlshreyas/processed/qualifying'))

# COMMAND ----------

#qualifying_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed_new.qualifying")

# COMMAND ----------

#overwrite_partition(qualifying_final, 'f1_processed_new', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final, 'f1_processed_dl', 'qualifying', processeddl_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
