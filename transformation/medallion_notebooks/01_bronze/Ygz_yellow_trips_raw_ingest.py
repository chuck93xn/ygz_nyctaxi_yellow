# Databricks notebook source
import os
import sys
from pathlib import Path

def setup_project_path():
    try:
        project_root = Path(__file__).resolve().parents[2]
        if str(project_root) not in sys.path:
            sys.path.append(str(project_root))
        return project_root
    except NameError:

        return None
PROJECT_ROOT = setup_project_path()


from datetime import date, datetime, timezone
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_target_yyyymm
from modules.transformation_layers.metadata import add_processed_timestamp
# COMMAND ----------

formatted_date = get_target_yyyymm(2)
file_path = f"/Volumes/nyctaxi_ygz/00_landing/data_sources/nyctaxi_yellow/{formatted_date}/"

# COMMAND ----------

# Read files for the specified month from the landing directory into a DataFrame
df = spark.read.format("parquet").load(file_path)

df = add_processed_timestamp(df)

# COMMAND ----------

# appending method
df.write.mode('append').saveAsTable('nyctaxi_ygz.01_bronze.yellow_trips_raw')