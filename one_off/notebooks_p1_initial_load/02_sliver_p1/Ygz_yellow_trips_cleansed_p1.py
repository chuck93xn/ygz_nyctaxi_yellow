# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.table("nyctaxi_ygz.01_bronze.yellow_trips_raw")

# COMMAND ----------

df.agg(
    F.max("tpep_pickup_datetime"),
    F.min("tpep_pickup_datetime"),
    F.max("tpep_dropoff_datetime"),
    F.min("tpep_dropoff_datetime")
    ).show()

# COMMAND ----------

df = df.filter("""
               tpep_pickup_datetime >= '2025-11-01' 
               AND tpep_pickup_datetime < '2026-05-01'
               AND tpep_dropoff_datetime >= '2025-11-01' 
               AND tpep_dropoff_datetime < '2026-05-01'
               """)

# COMMAND ----------

from pyspark.sql.functions import when, col

vendor_col = (
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
    .when(col("VendorID") == 2, "Curb Mobility, LLC")
    .when(col("VendorID") == 6, "Myle Technologies Inc")
    .when(col("VendorID") == 7, "Helix")
    .otherwise("Unknown")
    .alias("vendor")
)
print(vendor_col)

# COMMAND ----------

df = df.select(
    F.expr("""
        CASE
            WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC'
            WHEN VendorID = 2 THEN 'Curb Mobility, LLC'
            WHEN VendorID = 6 THEN 'Myle Technologies Inc'
            WHEN VendorID = 7 THEN 'Helix'
            ELSE 'Unknown'
        END
    """).alias("vendor"),
    
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    # Calculate trip duration in minutes
    
    F.expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)").alias("trip_duration"),

    "passenger_count",
    "trip_distance",

    # Decode rate codes into readable rate types
    when(F.col("RatecodeID") == 1, "Standard Rate")
      .when(F.col("RatecodeID") == 2, "JFK")
      .when(F.col("RatecodeID") == 3, "Newark")
      .when(F.col("RatecodeID") == 4, "Nassau or Westchester")
      .when(F.col("RatecodeID") == 5, "Negotiated Fare")
      .when(F.col("RatecodeID") == 6, "Group Ride")
      .otherwise("Unknown")
      .alias("rate_type"),
    
    "store_and_fwd_flag",
    # alias columns for consistent naming convention
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),
    
    # Decode payment types
    F.expr("""
        CASE
            WHEN payment_type = 0 THEN 'Flex Fare trip'
            WHEN payment_type = 1 THEN 'Credit card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No charge'
            WHEN payment_type = 4 THEN 'Dispute'
            WHEN payment_type = 6 THEN 'Voided trip'
            ELSE 'Unknown'
        END AS payment_type
    """),

    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    # alias columns for consistent naming convention
    F.col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processing_timestamp"
)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi_ygz.`02_silver`.yellow_trips_cleansed")