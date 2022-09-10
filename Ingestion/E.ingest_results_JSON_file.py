# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest results JSON file

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
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------


schema_results = StructType(fields=[StructField("resultId", IntegerType(), False),StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True), StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True), StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),StructField("statusId", StringType(), True)])

# COMMAND ----------

results = spark.read.schema(schema_results).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results)

# COMMAND ----------

results_final = results.selectExpr("resultId as result_id", "raceId as race_id", "driverId as driver_id", "constructorid as constructor_id", "number as number", "grid as grid", "position as position", "positionText as position_text", "positionOrder as position_order", "points as points", "laps as laps", "time as time", "milliseconds","fastestLap as fastest_lap", "rank as rank", "fastestLapTime as fastest_lap_time", "fastestLapSpeed as fastest_lap_speed" ).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_final = add_ingestion_date(results_final)

# COMMAND ----------

results_final = results_final.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

display(results_final)

# COMMAND ----------

#results_final.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/results")

# COMMAND ----------

#results_final.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed_new.results")

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#   if (spark._jsparkSession.catalog().tableExists("f1_processed_new.results")):
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed_new.results")

# COMMAND ----------

#overwrite_partition(results_final, 'f1_processed_new', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final, 'f1_processed_dl', 'results', processeddl_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id
# MAGIC FROM f1_processed_new.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------


