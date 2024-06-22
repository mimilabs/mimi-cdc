# Databricks notebook source
# MAGIC %md
# MAGIC ## CDC Data Download
# MAGIC
# MAGIC The data can be downloaded every Friday

# COMMAND ----------


import requests
from pathlib import Path
import datetime
from dateutil.relativedelta import *
import zipfile

# COMMAND ----------

t = datetime.datetime.now().strftime('%Y%m%d')

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

# MAGIC %md
# MAGIC ## NNDSS

# COMMAND ----------

url = "https://data.cdc.gov/api/views/x9gk-5huc/rows.csv?accessType=DOWNLOAD&api_foundry=true"
volumepath = "/Volumes/mimi_ws_1/cdc/src/nndss"
download_file(url, f"nndss_{t}.csv", volumepath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## NWSS

# COMMAND ----------

url = "https://data.cdc.gov/api/views/2ew6-ywp6/rows.csv?accessType=DOWNLOAD&api_foundry=true"
volumepath = "/Volumes/mimi_ws_1/cdc/src/nwss"
download_file(url, f"nwss_covid_{t}.csv", volumepath)

# COMMAND ----------

# MPox data
# blob:https://www.cdc.gov/01b061ee-6ee0-4bfc-bb09-d48413e44b23
# blob:https://www.cdc.gov/4fa9d5c1-ba63-4f2b-b521-b65731bb34b7
# The Mpox data URL changes every week
# The page is Javascript generated, and needs Selenium...


# COMMAND ----------

# MAGIC %md
# MAGIC ## VSRR - Drug Overdose Death counts

# COMMAND ----------

url = "https://data.cdc.gov/api/views/x9gk-5huc/rows.csv?accessType=DOWNLOAD&api_foundry=true"
url = "https://data.cdc.gov/api/views/xkb8-kh2a/rows.csv?accessType=DOWNLOAD&api_foundry=true"
volumepath = "/Volumes/mimi_ws_1/cdc/src/vsrr_drugoverdose"
download_file(url, f"vsrr_drugoverdose_{t}.csv", volumepath)

# COMMAND ----------


