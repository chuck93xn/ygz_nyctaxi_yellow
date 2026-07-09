# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
from pyspark.sql.functions import date_format, count, sum

# COMMAND ----------

(
    spark.read.table("nyctaxi_ygz.`01_bronze`.yellow_trips_raw")
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month"))
    .agg(count("*").alias("total_records"))
    .orderBy("year_month")
    .display()
)

# COMMAND ----------

(
    spark.read.table("nyctaxi_ygz.`02_silver`.yellow_trips_cleansed")
    .groupBy(date_format("pickup_datetime", "yyyy-MM").alias("year_month"))
    .agg(count("*").alias("total_records"))
    .orderBy("year_month")
    .display()
)

# COMMAND ----------

(
    spark.read.table("nyctaxi_ygz.`02_silver`.yellow_trips_enriched")
    .groupBy(date_format("pickup_datetime", "yyyy-MM").alias("year_month"))
    .agg(count("*").alias("total_records"))
    .orderBy("year_month")
    .display()
)

# COMMAND ----------

(
    spark.read.table("nyctaxi_ygz.`03_gold`.daily_trip_summary")
    .groupBy(date_format("pickup_date", "yyyy-MM").alias("year_month"))
    .agg(count("*").alias("total_records"))
    .orderBy("year_month")
    .display()
)

# COMMAND ----------

(
    spark.read.table("nyctaxi_ygz.`04_export`.yellow_trips_export")
    .groupBy("year_month")
    .agg(count("*").alias("total_records"))
    .orderBy("year_month")
    .display()
)

# COMMAND ----------

spark.read.table("nyctaxi_ygz.`02_silver`.taxi_zone_lookup").display()
