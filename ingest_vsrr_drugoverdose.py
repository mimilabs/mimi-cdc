# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the VSRR-DrugOverdose files
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

path = "/Volumes/mimi_ws_1/cdc/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "cdc" # delta table destination schema

# COMMAND ----------

tablename = "vsrr_drugoverdose"
writemode = "overwrite" # always overwrite; nndss weekly files contain historic data
files = []
for filepath in Path(f"{path}/{tablename}").glob("*"):
    files.append(filepath)
files = sorted(files, key=lambda x: x.stem[-8:], reverse=True)
file_latest = files[0]

# COMMAND ----------

pdf = pd.read_csv(file_latest, encoding='ISO-8859-1', dtype={"Percent Complete": str})
pdf.columns = change_header(pdf.columns)
pdf["mimi_src_file_name"] = file_latest.name
pdf["mimi_src_file_date"] = parse(file_latest.stem[-8:]).date()
pdf["mimi_dlt_load_date"] = datetime.today().date()
datestr = (pdf['month'] + " 1, " + pdf['year'].astype(str)).apply(lambda x: parse(x).date())
pdf["report_date"] = datestr

# COMMAND ----------

(spark.createDataFrame(pdf).write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
