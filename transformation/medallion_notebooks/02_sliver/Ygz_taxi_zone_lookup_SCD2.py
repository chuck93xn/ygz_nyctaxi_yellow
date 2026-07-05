# Databricks notebook source
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import col, when, lit, current_timestamp, asc, desc
from pyspark.sql.types import TimestampType, IntegerType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source
# MAGIC The newest record for lookup table. Get from volume parquet file

# COMMAND ----------

lookup_url = '/Volumes/nyctaxi_ygz/00_landing/data_sources/lookup/'
df = spark.read.format('csv').options(header='true', inferSchema='true').load(lookup_url)

# COMMAND ----------

df = df.select(
                col("LocationID").cast(IntegerType()).alias("location_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone"),
                col("service_zone"),
                current_timestamp().alias("effective_date"),
                lit(None).cast(TimestampType()).alias("end_date")
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Force updates and insertions
# MAGIC - **Skip afterward**. Just to see whether the data been update

# COMMAND ----------

# MAGIC %skip
# MAGIC # add a new record
# MAGIC df_new = spark.createDataFrame(
# MAGIC     [(0, "New Borough", "New Zone", "New Service Zone")],
# MAGIC     schema="location_id int, borough string, zone string, service_zone string"
# MAGIC ).withColumn("effective_date", current_timestamp()) \
# MAGIC  .withColumn("end_date", lit(None).cast("timestamp"))
# MAGIC df = df_new.union(df)
# MAGIC
# MAGIC # change the current record 
# MAGIC df = df.withColumn("borough", when(col("location_id")==1, "NEWARK AIRPORT").otherwise(col("borough")))

# COMMAND ----------

# MAGIC %skip
# MAGIC df.sort(asc("location_id")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Target table

# COMMAND ----------

dt = DeltaTable.forName(spark, "nyctaxi_Ygz.02_silver.taxi_zone_lookup")

# need to set this variable as time is continously changing
end_timestamp = datetime.now()

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("DESCRIBE HISTORY nyctaxi_ygz.02_silver.taxi_zone_lookup").display()

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("RESTORE nyctaxi_Ygz.02_silver.taxi_zone_lookup VERSION AS OF 11")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Check whether any existing rows need to be update
# MAGIC If any updates, set end_date from NULL to current_timestamp()

# COMMAND ----------

(
dt.alias("t")
    .merge(
        source    = df.alias("s"),
        condition = """
            t.location_id = s.location_id 
            AND t.end_date IS NULL 
            AND (
                t.borough != s.borough 
                OR t.zone != s.zone 
                OR t.service_zone != s.service_zone
                )
        """
    )
    .whenMatchedUpdate(
        set = {"t.end_date": lit(end_timestamp).cast(TimestampType())}
    )
.execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Insert new current versions
# MAGIC Get a list of all `location_id` need to be updated, then update and merge

# COMMAND ----------

# MAGIC %skip
# MAGIC dt.toDF().filter(f"end_date = '{end_timestamp}'").select("location_id").show()

# COMMAND ----------

# Get location_id column need to be updated
df_insert_id = dt.toDF().filter(f"end_date = '{end_timestamp}'").select("location_id")
# Make a list to store all location_id
insert_id_list = [row.location_id for row in df_insert_id.collect()]

# COMMAND ----------

# MAGIC %skip
# MAGIC print(insert_id_list)

# COMMAND ----------

if len(insert_id_list) == 0:
    print("No updated records to insert")
else:(
    dt.alias("t")
        .merge(
            source    = df.alias("s"),
            condition = f"s.location_id not in ({', '.join(map(str, insert_id_list))})"
            # condition = "s.location_id not in (1)"
        )
        .whenNotMatchedInsert(
            values = {"t.location_id": "s.location_id",
                    "t.borough": "s.borough",
                    "t.zone": "s.zone",
                    "t.service_zone": "s.service_zone",
                    "t.effective_date": current_timestamp(),
                    "t.end_date": lit(None).cast(TimestampType())}
        )
        .execute()
)

# COMMAND ----------

# MAGIC %skip
# MAGIC explain_condition = f"s.location_id not in ({', '.join(map(str, [101, 205, 308, 415]))})"
# MAGIC # map(str, [101, 205, 308, 415])
# MAGIC # 结果：['101', '205', '308', '415']
# MAGIC print(', '.join(map(str, [101, 205, 308, 415])))
# MAGIC print(explain_condition)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Insert brand-new rows

# COMMAND ----------

# %skip
dt.toDF().sort(asc("location_id")).display()


# COMMAND ----------

(
dt.alias("t")
    .merge(
        source    = df.alias("s"),
        condition = "t.location_id = s.location_id"
    )
    .whenNotMatchedInsert(
        values = {"t.location_id": "s.location_id",
                "t.borough": "s.borough",
                "t.zone": "s.zone",
                "t.service_zone": "s.service_zone",
                "t.effective_date": current_timestamp(),
                "t.end_date": lit(None).cast(TimestampType())}
    )
    .execute()
)