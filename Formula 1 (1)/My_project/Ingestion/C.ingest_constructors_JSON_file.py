# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Constructors.JSON file###

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

from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit

# COMMAND ----------

constructor = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json", schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING")

# COMMAND ----------

display(constructor)

# COMMAND ----------

constructor_new = constructor.drop(col('url'))

# COMMAND ----------

constructor_final = constructor_new.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final = add_ingestion_date(constructor_final)

# COMMAND ----------

display(constructor_final)

# COMMAND ----------

constructor_final.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

constructor_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed_new.constructors")

# COMMAND ----------

constructor_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed_dl.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
