# Databricks notebook source
import urllib.request
import os
import shutil

url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
response = urllib.request.urlopen(url)
dir_path = '/Volumes/nyctaxi_ygz/00_landing/data_sources/lookup'
os.makedirs(dir_path, exist_ok=True)
local_path = dir_path + '/taxi_zone_lookup.csv'
with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f)