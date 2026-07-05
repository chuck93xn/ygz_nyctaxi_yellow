# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

file_path = '/Volumes/nyctaxi_ygz/00_landing/data_sources/nyctaxi_yellow/*'
df = spark.read.format('parquet').load(file_path)
df = df.withColumn('processing_timestamp', current_timestamp())

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('nyctaxi_ygz.01_bronze.yellow_trips_raw')