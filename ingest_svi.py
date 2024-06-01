# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the NNDSS files
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_county_y2022;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_county_y2020;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_county_y2018;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_county_y2016;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_county_y2014;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_county_y2010;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_county_y2000;
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
# MAGIC ## County-level

# COMMAND ----------

tablename = "svi"
writemode = "overwrite" # always overwrite; nndss weekly files contain historic data
files = []
for filepath in Path(f"{path}/{tablename}").glob("*_county.csv"):
    dt = parse(filepath.stem.split('_')[1] + '-12-31').date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    str_vars = {"FIPS": str, "ST": str}
    if item[0].year == 2010:
        str_vars = {"FIPS": str, "STATE": str}
    elif item[0].year == 2000:
        str_vars = {"STATE_FIPS": str,
                "CNTY_FIPS": str,
                "STCOFIPS": str}
    pdf = pd.read_csv(item[1], dtype=str_vars)
    pdf.columns = change_header(pdf.columns)
    pdf["_input_file_date"] = item[0]
    df = spark.createDataFrame(pdf)
    tablename_year = f"{tablename}_county_y{item[0].year}"
    (df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"mimi_ws_1.cdc.{tablename_year}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Census-Tract

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_censustract_y2022;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_censustract_y2020;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_censustract_y2018;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_censustract_y2016;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_censustract_y2014;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_censustract_y2010;
# MAGIC --DROP TABLE mimi_ws_1.cdc.svi_censustract_y2000;

# COMMAND ----------

tablename = "svi"
writemode = "overwrite" # always overwrite; nndss weekly files contain historic data
files = []
for filepath in Path(f"{path}/{tablename}").glob("*.csv"):
    if filepath.stem[-6:] == "county":
        continue
    dt = parse(filepath.stem.split('_')[1] + '-12-31').date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    str_vars = {"FIPS": str, "ST": str, "STCNTY": str, "TRACTCE": str}
    if item[0].year == 2010:
        str_vars = {"FIPS": str, "STATE": str, "STCOFIPS": str}
    elif item[0].year == 2000:
        str_vars = {"STATE_FIPS": str,
                "CNTY_FIPS": str,
                "STCOFIPS": str, 
                "TRACT": str,
                "FIPS": str}
    pdf = pd.read_csv(item[1], dtype=str_vars)
    pdf.columns = change_header(pdf.columns)
    pdf["_input_file_date"] = item[0]
    df = spark.createDataFrame(pdf)
    tablename_year = f"{tablename}_censustract_y{item[0].year}"
    (df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"mimi_ws_1.cdc.{tablename_year}"))

# COMMAND ----------


