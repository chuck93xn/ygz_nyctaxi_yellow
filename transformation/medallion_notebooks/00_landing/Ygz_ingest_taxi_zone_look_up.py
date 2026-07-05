# Databricks notebook source
import urllib.request
import os
import shutil

try:
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    response = urllib.request.urlopen(url)
    dir_path = '/Volumes/nyctaxi_ygz/00_landing/data_sources/lookup'
    # If the file is already exist and no change, still overwrite the file
    os.makedirs(dir_path, exist_ok=True)
    local_path = dir_path + '/taxi_zone_lookup.csv'
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)
    # Set the value of the task output for if_else task
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File succesfully uploaded") 

except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File download failed: {str(e)}")

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC import urllib.request
# MAGIC import os
# MAGIC import shutil
# MAGIC
# MAGIC try:
# MAGIC     # Construct the URL for the Parquet file corresponding to this month
# MAGIC     url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
# MAGIC
# MAGIC     # Open a connection and stream the remote file
# MAGIC     response = urllib.request.urlopen(url)
# MAGIC
# MAGIC     # Define and create the local directory for this date's data
# MAGIC     dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/lookup"
# MAGIC     os.makedirs(dir_path, exist_ok=True)
# MAGIC
# MAGIC     # Define the full path for the downloaded file
# MAGIC     local_path = f"{dir_path}/taxi_zone_lookup.csv"
# MAGIC
# MAGIC     # Save the streamed content to the local file in binary mode
# MAGIC     with open(local_path, 'wb') as f:
# MAGIC         shutil.copyfileobj(response, f)  # Copy data from response to file
# MAGIC     
# MAGIC     dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
# MAGIC     print("File succesfully uploaded")
# MAGIC except Exception as e:
# MAGIC     dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
# MAGIC     print(f"File download failed: {str(e)}")