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

import urllib.request
import shutil
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
from modules.data_loader_landing.file_downloader import download_file
from modules.utils.date_utils import get_target_yyyymm

# COMMAND ----------

formatted_date = get_target_yyyymm(2)

dir_path = f"/Volumes/nyctaxi_ygz/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"

local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

# COMMAND ----------

try:
    # Check if the file already exists
    dbutils.fs.ls(local_path)

    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already downloaded, aborting downstream tasks")
except:
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

        download_file(url, dir_path, local_path)
        
        # Set continue_downstream to yes if the file was loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File succesfully uploaded in current run")
        
    except Exception as e:
        # Set continue downstream to no if the file was not loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File download failed: {str(e)}")