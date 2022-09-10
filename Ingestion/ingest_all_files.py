# Databricks notebook source
v_resultA = dbutils.notebook.run("A.ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_resultA

# COMMAND ----------

v_resultB = dbutils.notebook.run("B.ingest_races_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_resultB

# COMMAND ----------

v_resultC = dbutils.notebook.run("C.ingest_constructors_JSON_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_resultC

# COMMAND ----------

v_resultD = dbutils.notebook.run("D.ingest_drivers_file_JSON", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_resultD

# COMMAND ----------

v_resultE = dbutils.notebook.run("E.ingest_results_JSON_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_resultE

# COMMAND ----------

v_resultF = dbutils.notebook.run("F.ingest_pit_stops_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_resultF

# COMMAND ----------

v_resultG = dbutils.notebook.run("G.ingest_lap_times_folder", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_resultG

# COMMAND ----------

v_resultH = dbutils.notebook.run("H.ingest_qualifying_folder", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_resultH

# COMMAND ----------


