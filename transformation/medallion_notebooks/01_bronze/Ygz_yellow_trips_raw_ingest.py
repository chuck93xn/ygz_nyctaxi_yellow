# Databricks notebook source
from datetime import date, datetime, timezone
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
date_ingest = two_months_ago.strftime("%Y-%m")
file_path = f"/Volumes/nyctaxi_ygz/00_landing/data_sources/nyctaxi_yellow/{date_ingest}/"

# COMMAND ----------

# Read files for the specified month from the landing directory into a DataFrame
df = spark.read.format("parquet").load(file_path)

df = df.withColumn('processing_timestamp', current_timestamp())

# COMMAND ----------

# appending method
df.write.mode('append').saveAsTable('nyctaxi_ygz.01_bronze.yellow_trips_raw')