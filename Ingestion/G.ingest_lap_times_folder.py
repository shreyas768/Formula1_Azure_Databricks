# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

schema_lap_times = StructType(fields=[StructField("raceId", IntegerType(), False),StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

lap_times = spark.read.schema(schema_lap_times).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

lap_times = add_ingestion_date(lap_times)

# COMMAND ----------

display(lap_times)

# COMMAND ----------

lap_times.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final = lap_times.selectExpr("driverId as driver_id" , "raceId as race_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#lap_times_final.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

#lap_times_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed_new.lap_times")

# COMMAND ----------

#overwrite_partition(lap_times_final, 'f1_processed_new', 'lap_times', 'race_id')

# COMMAND ----------

merge_cond = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final, 'f1_processed_dl', 'lap_times', processeddl_folder_path, merge_cond, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
