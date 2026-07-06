# Databricks notebook source
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import urllib.request
import shutil
from modules.data_loader_landing.file_downloader import download_file

try:
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

    dir_path = '/Volumes/nyctaxi_ygz/00_landing/data_sources/lookup'

    local_path = dir_path + '/taxi_zone_lookup.csv'
    
    download_file(url, dir_path, local_path)

    # Set the value of the task output for if_else task
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File succesfully uploaded") 

except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File download failed: {str(e)}")
