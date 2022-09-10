# Databricks notebook source
##Read Races.csv file##

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

Races = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(Races)

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceID", IntegerType(), False), StructField("year", IntegerType(), True), StructField("round", IntegerType(), True),
                                  StructField("circuitID", IntegerType(), True), StructField("name", StringType(), True), StructField("date", DateType(), True),
                                  StructField("time", StringType(), True), StructField("url", StringType(), True) ])

# COMMAND ----------

Races_new = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv").withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(Races_new)

# COMMAND ----------

Races_new = Races_new.drop('url')

# COMMAND ----------

display(Races_new)

# COMMAND ----------

Races_timestamp = Races_new.withColumn("ingestion_date", current_timestamp()).withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(Races_timestamp)

# COMMAND ----------

Races_New1 = Races_timestamp.selectExpr("raceID as race_id", "year as race_year", "round as round", "circuitID as circuit_id", "name as name", "ingestion_date as ingestion_date", "race_timestamp as race_timestamp" )

# COMMAND ----------

display(Races_New1)

# COMMAND ----------

Races_New1.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlshreyas/processed/races

# COMMAND ----------

Races_New1.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed_new.races")

# COMMAND ----------

Races_New1.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed_dl.races")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


