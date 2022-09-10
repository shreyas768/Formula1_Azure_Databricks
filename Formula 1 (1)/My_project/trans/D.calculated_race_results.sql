-- Databricks notebook source
USE f1_processed_new;

-- COMMAND ----------

create table f1_presentation.calculated_race_results
using parquet
select races.race_year, constructors.name as team_name, drivers.name as driver_name, results.position, results.points, 11- results.position as calculated_points
from results
join f1_processed_new.drivers on (results.driver_id = drivers.driver_id)
join f1_processed_new.constructors on (results.constructor_id = constructors.constructor_id)
join f1_processed_new.races on (results.race_id = races.race_id)
where results.position < 11

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------


