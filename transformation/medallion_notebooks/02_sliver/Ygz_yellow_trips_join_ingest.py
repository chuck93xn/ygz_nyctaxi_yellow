# Databricks notebook source
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col

# COMMAND ----------

start_date = date.today().replace(day=1) - relativedelta(months=2)

# COMMAND ----------

df_trips = spark.read.table("nyctaxi_ygz.02_silver.yellow_trips_cleansed").filter(f"pickup_datetime > '{start_date}'")
df_zones = spark.read.table("nyctaxi_ygz.`02_silver`.taxi_zone_lookup")

# COMMAND ----------

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

# MAGIC %skip
# MAGIC from pyspark.sql.functions import col, cast, count, asc
# MAGIC df_final.groupBy(df.pickup_datetime.cast("date").alias("pickup_date")).agg(count("*")).sort(asc("pickup_date")).show()

# COMMAND ----------

df_final.write.mode("append").saveAsTable("nyctaxi_ygz.`02_silver`.yellow_trips_enriched")