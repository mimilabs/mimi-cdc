# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the NNDSS files
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

from datetime import date
path = "/Volumes/mimi_ws_1/cdc/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "cdc" # delta table destination schema

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
pdf["mimi_src_file_name"] = file_latest.name
pdf["mimi_src_file_date"] = parse(file_latest.stem[-8:]).date()
pdf["mimi_dlt_load_date"] = datetime.today().date()
pdf["report_date"] = pdf.apply(lambda x: date.fromisocalendar(x["current_mmwr_year"], 
                                                            x["mmwr_week"], 
                                                            4), 
                            axis=1)

# COMMAND ----------

(spark.createDataFrame(pdf).write
        .format('delta')
        .mode(writemode)
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------


