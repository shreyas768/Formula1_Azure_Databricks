# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest DriversJSON File##

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

schema_name = StructType(fields=[StructField("forename", StringType(), True),StructField("surname", StringType(), True)])

# COMMAND ----------

schema_drivers = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", schema_name),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers = spark.read.schema(schema_drivers).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers = add_ingestion_date(drivers)

# COMMAND ----------

drivers_columns = drivers.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref").withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_columns)

# COMMAND ----------

drivers_final = drivers_columns.drop(col('url'))

# COMMAND ----------

display(drivers_final)

# COMMAND ----------

drivers_final.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

drivers_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed_new.drivers")

# COMMAND ----------

drivers_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed_dl.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
