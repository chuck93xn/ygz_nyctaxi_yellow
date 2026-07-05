# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round

# COMMAND ----------

df = spark.read.table("nyctaxi_ygz.`02_silver`.yellow_trips_enriched")

# COMMAND ----------

df.display()

# COMMAND ----------

# Aggregate trip data by pickup date with key metrics

df_summary = df.groupBy(df.pickup_datetime.cast("date").alias("pickup_date") )\
                .agg(
                    count("*").alias("total_trips"),                             # total number of trips per day
                    round(avg("passenger_count"), 1).alias("average_passengers"), # average passengers per trip
                    round(avg("trip_duration"), 1).alias("average_trip_duration_mins"),
                    round(avg("trip_distance"), 1).alias("average_distance"),     # average trip distance (miles)
                    round(avg("fare_amount"), 2).alias("average_fare_per_trip"),   # average fare per trip ($)
                    max("fare_amount").alias("max_fare"),                         # highest single-trip fare
                    min("fare_amount").alias("min_fare"),                         # lowest single-trip fare
                    round(sum("total_amount"), 2).alias("total_revenue")          # total revenue for the day ($)
                )

# COMMAND ----------

df_summary.sort("pickup_date").display()

# COMMAND ----------

df_summary.write.mode("overwrite").saveAsTable("nyctaxi_ygz.03_gold.daily_trip_summary")