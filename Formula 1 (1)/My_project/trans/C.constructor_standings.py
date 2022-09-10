# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce constructor standings

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank, asc
from pyspark.sql.window import Window

# COMMAND ----------

constructor_standings = race_results.groupBy("race_year", "team").agg(sum("points").alias("total_points"),count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings.filter("race_year = 2020"))

# COMMAND ----------

constructor_rank_spc = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final = constructor_standings.withColumn("rank", rank().over(constructor_rank_spc))

# COMMAND ----------

display(final.filter("race_year = 2020"))

# COMMAND ----------

final.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------


