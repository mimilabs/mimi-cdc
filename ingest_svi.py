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

tablename = "nndss"
writemode = "overwrite" # always overwrite; nndss weekly files contain historic data
files = []
for filepath in Path(f"{path}/{tablename}").glob("*"):
    files.append(filepath)
files = sorted(files, key=lambda x: x.stem[-8:], reverse=True)
file_latest = files[0]

# COMMAND ----------

pdf = pd.read_csv(file_latest, encoding='ISO-8859-1')
pdf.columns = change_header(pdf.columns)
pdf["report_date"] = pdf.apply(lambda x: date.fromisocalendar(x["current_mmwr_year"], 
                                                            x["mmwr_week"], 
                                                            4), 
                            axis=1)

# COMMAND ----------

(spark.createDataFrame(pdf).write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------


