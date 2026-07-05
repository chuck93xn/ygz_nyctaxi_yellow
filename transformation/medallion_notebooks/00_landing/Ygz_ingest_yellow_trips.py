# Databricks notebook source
import urllib.request
import os
import shutil
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
date_ingest = two_months_ago.strftime("%Y-%m")

dir_path = f"/Volumes/nyctaxi_ygz/00_landing/data_sources/nyctaxi_yellow/{date_ingest}"

local_path = f"{dir_path}/yellow_tripdata_{date_ingest}.parquet"

# COMMAND ----------

try:
    # Check if the file already exists
    dbutils.fs.ls(local_path)

    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already downloaded, aborting downstream tasks")
except:
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date_ingest}.parquet"
        response = urllib.request.urlopen(url)
        os.makedirs(dir_path, exist_ok=True)
        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f)
        
        # Set continue_downstream to yes if the file was loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File succesfully uploaded in current run")
        
    except Exception as e:
        # Set continue downstream to no if the file was not loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File download failed: {str(e)}")

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC # Obtains the year-month for 2 months prior to the current month in yyyy-MM format
# MAGIC two_months_ago = date.today() - relativedelta(months=2)
# MAGIC formatted_date = two_months_ago.strftime("%Y-%m")
# MAGIC
# MAGIC # Define the local directory for this date's data
# MAGIC dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
# MAGIC
# MAGIC # Define the full path for the downloaded file
# MAGIC local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"
# MAGIC
# MAGIC try:
# MAGIC     # Check if the file already exists
# MAGIC     dbutils.fs.ls(local_path)
# MAGIC
# MAGIC     # If the file already exists then set continue_downstream to no
# MAGIC     dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
# MAGIC     print("File already downloaded, aborting downstream tasks")
# MAGIC except:
# MAGIC     try:
# MAGIC         # Construct the URL for the Parquet file corresponding to this month
# MAGIC         url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"
# MAGIC
# MAGIC         # Open a connection and stream the remote file
# MAGIC         response = urllib.request.urlopen(url)
# MAGIC
# MAGIC         # Create the local directory for this date's data
# MAGIC         os.makedirs(dir_path, exist_ok=True)
# MAGIC
# MAGIC         # Save the streamed content to the local file in binary mode
# MAGIC         with open(local_path, 'wb') as f:
# MAGIC             shutil.copyfileobj(response, f)  # Copy data from response to file
# MAGIC         
# MAGIC         # Set continue_downstream to yes if the file was loaded
# MAGIC         dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
# MAGIC         print("File succesfully uploaded in current run")
# MAGIC     except Exception as e:
# MAGIC         # Set continue downstream to no if the file was not loaded
# MAGIC         dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
# MAGIC         print(f"File download failed: {str(e)}")