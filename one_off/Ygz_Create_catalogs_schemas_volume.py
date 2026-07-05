# Databricks notebook source
managed_location = 'abfss://unity-catalog-storage@dbstoragempd7eumelu6mk.dfs.core.windows.net/7405607794917680'
spark.sql(f"CREATE CATALOG IF NOT EXISTS nyctaxi_Ygz MANAGED LOCATION '{managed_location}'")

# COMMAND ----------

spark.sql("create schema if not exists nyctaxi_Ygz.00_landing")
spark.sql("create schema if not exists nyctaxi_Ygz.01_bronze")
# spark.sql("create schema if not exists nyctaxi_chuck.02_silver")
# spark.sql("create schema if not exists nyctaxi_chuck.03_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists nyctaxi_Ygz.02_silver;
# MAGIC create schema if not exists nyctaxi_Ygz.03_gold;

# COMMAND ----------

spark.sql("create volume if not exists nyctaxi_Ygz.00_landing.data_sources")