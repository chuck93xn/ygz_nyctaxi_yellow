# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

lookup_url = '/Volumes/nyctaxi_ygz/00_landing/data_sources/lookup/'
df = spark.read.format('csv').options(header='true', inferSchema='true').load(lookup_url)

# COMMAND ----------

# 注意赋值，只能run一次，否则会报错
df = df.select(
                F.col("LocationID").cast(T.IntegerType()).alias("location_id"),
                F.col("Borough").alias("borough"),
                F.col("Zone").alias("zone"),
                F.col("service_zone"),
                F.current_timestamp().alias("effective_date"),
                F.lit(None).cast(T.TimestampType()).alias("end_date")
            )

# COMMAND ----------

# 注意赋值，只能run一次，否则会报错
df = df.selectExpr(
        "CAST(LocationID AS INT) AS location_id",
        "Borough AS borough",
        "Zone AS zone",
        "service_zone",
        "current_timestamp() AS effective_date",
        "CAST(NULL AS TIMESTAMP) AS end_date"
)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi_ygz.02_silver.taxi_zone_lookup")

# COMMAND ----------

df.display()