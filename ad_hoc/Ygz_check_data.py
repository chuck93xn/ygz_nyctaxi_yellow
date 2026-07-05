# Databricks notebook source
from pyspark.sql.functions import *

df = spark.read.table("nyctaxi_ygz.`02_silver`.yellow_trips_enriched")

# COMMAND ----------

df.agg(
    max("pickup_datetime"),
    min("pickup_datetime"),
    max("dropoff_datetime"),
    min("dropoff_datetime")
    ).display()