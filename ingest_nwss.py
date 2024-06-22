# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the NWSS files
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

path = "/Volumes/mimi_ws_1/cdc/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "cdc" # delta table destination schema

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
pdf["mimi_src_file_name"] = file_latest.name
pdf["mimi_src_file_date"] = parse(file_latest.stem[-8:]).date()
pdf["mimi_dlt_load_date"] = datetime.today().date()

# COMMAND ----------

(spark.createDataFrame(pdf).write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
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
pdf["mimi_src_file_name"] = file_latest.name
pdf["mimi_src_file_date"] = parse(file_latest.stem[-8:]).date()
pdf["mimi_dlt_load_date"] = datetime.today().date()


# COMMAND ----------

(spark.createDataFrame(pdf).write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}_mpox"))

# COMMAND ----------


