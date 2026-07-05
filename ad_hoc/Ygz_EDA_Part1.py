# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.table("nyctaxi_ygz.`02_silver`.yellow_trips_enriched")

# COMMAND ----------

df.select("vendor").distinct().show()

# COMMAND ----------

# Most revenue venders
df.groupBy("vendor")\
    .agg(round(sum("total_amount"),2).alias("total_revenue"))\
        .orderBy(desc("total_revenue")).show()

# COMMAND ----------

(
df.groupBy("vendor")
    .agg(round(sum("total_amount"),2).alias("total_revenue"))
    .orderBy(desc("total_revenue"))
    .show()
)

# COMMAND ----------

# Most popular pick up zone
df.groupBy("pu_zone")\
    .agg(count("pu_zone").alias("total_pickups"))\
        .orderBy(desc("total_pickups")).show()

# COMMAND ----------

# Most common journery
journey_col = concat(
    "pu_zone",
    lit(" -> "),
    "do_zone"
).alias("journey")

(
    df
    .groupBy(journey_col)
    .agg(
        count("*").alias("total_journey")
    )
    .orderBy(
        desc("total_journey")
    )
    .show()
)