# Databricks notebook source
# MAGIC %md
# MAGIC ## CDC Places

# COMMAND ----------


import requests
from pathlib import Path
import datetime
from dateutil.relativedelta import *
import zipfile

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

url = "https://data.cdc.gov/api/views/cwsq-ngmh/rows.csv?accessType=DOWNLOAD&api_foundry=true"
volumepath = "/Volumes/mimi_ws_1/cdc/src/places"
download_file(url, f"places_censustract_2023.csv", volumepath)

# COMMAND ----------

url = "https://data.cdc.gov/api/views/swc5-untb/rows.csv?accessType=DOWNLOAD&api_foundry=true"
volumepath = "/Volumes/mimi_ws_1/cdc/src/places"
download_file(url, f"places_county_2023.csv", volumepath)

# COMMAND ----------

url = "https://data.cdc.gov/api/views/qnzd-25i4/rows.csv?accessType=DOWNLOAD&api_foundry=true"
volumepath = "/Volumes/mimi_ws_1/cdc/src/places"
download_file(url, f"places_zcta_2023.csv", volumepath)

# COMMAND ----------


