# Databricks notebook source
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, when, min, max, expr

# COMMAND ----------

start_date = date.today().replace(day=1) - relativedelta(months=2)
end_date = date.today().replace(day=1) - relativedelta(months=1)

# COMMAND ----------

df = (
    spark.read.table("nyctaxi_ygz.01_bronze.yellow_trips_raw")
    .filter(f"tpep_pickup_datetime >= '{start_date}' AND tpep_pickup_datetime < '{end_date}'")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select and transform fields
# MAGIC ### Business Mapping

# COMMAND ----------

vendor = expr("""
        CASE
            WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC'
            WHEN VendorID = 2 THEN 'Curb Mobility, LLC'
            WHEN VendorID = 6 THEN 'Myle Technologies Inc'
            WHEN VendorID = 7 THEN 'Helix'
            ELSE 'Unknown'
        END
    """).alias("vendor")

# COMMAND ----------

rate_type = (
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

payment_type = expr("""
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
# MAGIC ### Rename Fields

# COMMAND ----------

pickup_datetime = col("tpep_pickup_datetime").alias("pickup_datetime")
dropoff_datetime = col("tpep_dropoff_datetime").alias("dropoff_datetime")
pu_location_id = col("PULocationID").alias("pu_location_id")
do_location_id = col("DOLocationID").alias("do_location_id")
airport_fee = col("Airport_fee").alias("airport_fee")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculated Fields

# COMMAND ----------

trip_duration = expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)").alias("trip_duration")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Select

# COMMAND ----------

df = df.select(
    vendor,
    pickup_datetime,
    dropoff_datetime,
    trip_duration,
    "passenger_count",
    "trip_distance",
    rate_type,
    "store_and_fwd_flag",
    pu_location_id,
    do_location_id,
    payment_type,
    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    airport_fee,
    "cbd_congestion_fee",
    "processing_timestamp"
)

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi_ygz.`02_silver`.yellow_trips_cleansed")