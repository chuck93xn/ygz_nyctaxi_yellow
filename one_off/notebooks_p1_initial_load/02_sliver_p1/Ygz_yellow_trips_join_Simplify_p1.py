# Databricks notebook source
df_trips = spark.read.table("nyctaxi_ygz.`02_silver`.yellow_trips_cleansed")
df_zones = spark.read.table("nyctaxi_ygz.`02_silver`.taxi_zone_lookup")

# COMMAND ----------

from pyspark.sql.functions import col

t = df_trips.alias("t")
pu = df_zones.alias("pu")
do = df_zones.alias("do")

df = (
    t
    .join(pu, col("t.pu_location_id") == col("pu.location_id"), "left")
    .join(do, col("t.do_location_id") == col("do.location_id"), "left")
    .select(
        "t.*",
        col("pu.borough").alias("pu_borough"),
        col("pu.zone").alias("pu_zone"),
        col("do.borough").alias("do_borough"),
        col("do.zone").alias("do_zone")
    )
)
df_final = df.drop("pu_location_id", "do_location_id")

# COMMAND ----------

df_final.count()

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("nyctaxi_ygz.`02_silver`.yellow_trips_enriched")