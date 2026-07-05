# Databricks notebook source
# basic web request
import urllib.request
# create directory, file path
import os
# file operations
import shutil

# COMMAND ----------

# Demo of how to write single url file into the local file path
url_April = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-04.parquet'

# store the binary content of the url(file) in a variable
response = urllib.request.urlopen(url_April)

# create a directory to store the downloaded file
dir_path = '/Volumes/nyctaxi_ygz/00_landing/data_sources/nyctaxi_yellow/2026-04'
os.makedirs(dir_path, exist_ok=True)

# local file path - dir_path + filename
local_path = dir_path + '/yellow_tripdata_2026-04.parquet'

# 打开一个本地文件（写入模式），然后把 response 里的所有二进制内容，一块一块复制进去，最后自动关闭(with)文件。
# Python 下载大文件的标准写法。
# write the response into the local file path
# open the path in binary write mode
with open(local_path, 'wb') as f:
# Copy the response into the file
    shutil.copyfileobj(response, f)