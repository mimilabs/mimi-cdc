# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the NNDSS files
# MAGIC

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
from datetime import date

path = "/Volumes/mimi_ws_1/cdc/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "cdc" # delta table destination schema
def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID

# COMMAND ----------

tablename = "nwss"
writemode = "overwrite" # always overwrite; nndss weekly files contain historic data
files = []
for filepath in Path(f"{path}/{tablename}").glob("nwss_covid*"):
    files.append(filepath)
files = sorted(files, key=lambda x: x.stem[-8:], reverse=True)
file_latest = files[0]

# COMMAND ----------

pdf = pd.read_csv(file_latest, encoding='ISO-8859-1', 
                  dtype={"wwtp_id": str, 
                         "county_fips": str})
pdf.columns = change_header(pdf.columns)
pdf["date_start"] = pd.to_datetime(pdf["date_start"]).dt.date
pdf["date_end"] = pd.to_datetime(pdf["date_end"]).dt.date
pdf["first_sample_date"] = pd.to_datetime(pdf["first_sample_date"]).dt.date

# COMMAND ----------

(spark.createDataFrame(pdf).write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{tablename}_covid"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MPOX

# COMMAND ----------

tablename = "nwss"
writemode = "overwrite" # always overwrite; nndss weekly files contain historic data
files = []
for filepath in Path(f"{path}/{tablename}").glob("nwss_mpox*"):
    files.append(filepath)
files = sorted(files, key=lambda x: x.stem[-8:], reverse=True)
file_latest = files[0]

# COMMAND ----------

pdf = pd.read_csv(file_latest, encoding='ISO-8859-1', 
                  dtype={"wwtp_id": str, 
                         "county_fips": str},
                  parse_dates=["submission_date"])
pdf.columns = change_header(pdf.columns)
pdf["sample_collect_date"] = pd.to_datetime(pdf["sample_collect_date"]).dt.date
pdf["first_sample_collect_date"] = pd.to_datetime(pdf["first_sample_collect_date"]).dt.date
(spark.createDataFrame(pdf).write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{tablename}_mpox"))

# COMMAND ----------


