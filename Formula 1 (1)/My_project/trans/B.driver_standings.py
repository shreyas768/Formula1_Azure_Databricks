# Databricks notebook source
# MAGIC %md
# MAGIC ## Produce driver standings

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank, asc
from pyspark.sql.window import Window


# COMMAND ----------

display(race_results )

# COMMAND ----------

driver_standings = race_results.groupBy("race_year", "driver_name", "driver_nationality", "team").agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings.filter("race_year = 2020"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final = driver_standings.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final.filter("race_year = 2020"))

# COMMAND ----------

final.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------


