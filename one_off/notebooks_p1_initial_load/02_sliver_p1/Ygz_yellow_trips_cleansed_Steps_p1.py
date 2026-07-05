# Databricks notebook source
# MAGIC %md
# MAGIC ## Separate Each step for the future use for functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read & Filter

# COMMAND ----------

df = spark.read.table("nyctaxi_ygz.01_bronze.yellow_trips_raw")

# COMMAND ----------

from pyspark.sql.functions import max, min

# COMMAND ----------

df.agg(
    max("tpep_pickup_datetime"),
    min("tpep_pickup_datetime"),
    max("tpep_dropoff_datetime"),
    min("tpep_dropoff_datetime")
    ).show()

# COMMAND ----------

df = df.filter("""
               tpep_pickup_datetime >= '2025-11-01' 
               AND tpep_pickup_datetime < '2026-05-01'
               AND tpep_dropoff_datetime >= '2025-11-01' 
               AND tpep_dropoff_datetime < '2026-05-01'
               """)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Set column transformations

# COMMAND ----------

from pyspark.sql.functions import when, col, expr

# COMMAND ----------

# MAGIC %md
# MAGIC Business Mapping

# COMMAND ----------

vendor_col = expr("""
        CASE
            WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC'
            WHEN VendorID = 2 THEN 'Curb Mobility, LLC'
            WHEN VendorID = 6 THEN 'Myle Technologies Inc'
            WHEN VendorID = 7 THEN 'Helix'
            ELSE 'Unknown'
        END
    """).alias("vendor")

# COMMAND ----------

rate_type_col = (
    when(col("RatecodeID") == 1, "Standard Rate")
    .when(col("RatecodeID") == 2, "JFK")
    .when(col("RatecodeID") == 3, "Newark")
    .when(col("RatecodeID") == 4, "Nassau or Westchester")
    .when(col("RatecodeID") == 5, "Negotiated Fare")
    .when(col("RatecodeID") == 6, "Group Ride")
    .otherwise("Unknown")
    .alias("rate_type")
)

# COMMAND ----------

payment_type_col = expr("""
        CASE
            WHEN payment_type = 0 THEN 'Flex Fare trip'
            WHEN payment_type = 1 THEN 'Credit card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No charge'
            WHEN payment_type = 4 THEN 'Dispute'
            WHEN payment_type = 6 THEN 'Voided trip'
            ELSE 'Unknown'
        END AS payment_type
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename Fields

# COMMAND ----------

pickup_datetime_col = col("tpep_pickup_datetime").alias("pickup_datetime")
dropoff_datetime_col = col("tpep_dropoff_datetime").alias("dropoff_datetime")
pu_location_id_col = col("PULocationID").alias("pu_location_id")
do_location_id_col = col("DOLocationID").alias("do_location_id")
airport_fee_col = col("Airport_fee").alias("airport_fee")

# COMMAND ----------

# MAGIC %md
# MAGIC Calculated Fields

# COMMAND ----------

trip_duration_col = expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)").alias("trip_duration")

# COMMAND ----------

df = df.select(
    vendor_col,
    pickup_datetime_col,
    dropoff_datetime_col,
    trip_duration_col,
    "passenger_count",
    "trip_distance",
    rate_type_col,
    "store_and_fwd_flag",
    pu_location_id_col,
    do_location_id_col,
    payment_type_col,
    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    airport_fee_col,
    "cbd_congestion_fee",
    "processing_timestamp"
)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi_ygz.`02_silver`.yellow_trips_cleansed")