# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest circuits.csv file##

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

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlshreyas/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits)

# COMMAND ----------



# COMMAND ----------

circuits.printSchema()

# COMMAND ----------

circuits_new = circuits.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed = circuits_new.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------



# COMMAND ----------

circuits_final = add_ingestion_date(circuits_renamed)

# COMMAND ----------

circuits_final.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

circuits_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed_new.circuits")

# COMMAND ----------

circuits_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed_dl.circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processedtbl_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed_new.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")
