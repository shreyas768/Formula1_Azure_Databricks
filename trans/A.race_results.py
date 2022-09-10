# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read all the data 

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp as cts

# COMMAND ----------

drivers = spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("number", "driver_number").withColumnRenamed("name", "driver_name").withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

display(drivers)

# COMMAND ----------

constructors = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team") 

# COMMAND ----------

display(constructors)

# COMMAND ----------

circuits = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

display(circuits)

# COMMAND ----------

races = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

display(races)

# COMMAND ----------

results = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time", "race_time") 

# COMMAND ----------



# COMMAND ----------

display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits df to race df

# COMMAND ----------

race_circuits = races.join(circuits, races.circuit_id == circuits.circuit_id, "inner").select(races.race_id, races.race_year, races.race_name, races.race_date, circuits.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

# COMMAND ----------

race_results = results.join(race_circuits, results.race_id == race_circuits.race_id).join(drivers, results.driver_id == drivers.driver_id).join(constructors, results.constructor_id == constructors.constructor_id)

# COMMAND ----------

display(race_results)

# COMMAND ----------

final = race_results.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position").withColumn("created_date", cts())

# COMMAND ----------

display(final)

# COMMAND ----------

final.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


