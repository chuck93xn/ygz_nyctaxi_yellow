# Databricks notebook source
import urllib.request
import os
import shutil
from datetime import datetime
from dateutil.relativedelta import relativedelta

end_date = datetime.now() - relativedelta(months=3)
start_date = end_date - relativedelta(months=5)
date_process = []
current_date = start_date
while current_date <= end_date:
    date_process.append(current_date.strftime('%Y-%m'))
    next_month = current_date.month % 12 + 1
    next_year = current_date.year + (current_date.month // 12)
    current_date = datetime(next_year, next_month, 1)
print(date_process)

# COMMAND ----------

for date in date_process:
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet'
    response = urllib.request.urlopen(url)
    dir_path = f'/Volumes/nyctaxi_ygz/00_landing/data_sources/nyctaxi_yellow/{date}'
    os.makedirs(dir_path, exist_ok=True)
    local_path = dir_path + f'/yellow_tripdata_{date}.parquet'
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)