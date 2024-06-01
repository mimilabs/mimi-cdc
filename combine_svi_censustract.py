# Databricks notebook source
# MAGIC %md
# MAGIC # Combine the multi-year SVI files
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit, split, to_date, trim

# COMMAND ----------

#.withColumn("state_name", col("state_name"))
#.withColumn("state_abbr", col("state_abbr"))
df00 = (spark.read.table("mimi_ws_1.cdc.svi_censustract_y2000")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("usg1tp", "rpl_socioeconomic")
            .withColumnRenamed("usg2tp", "rpl_householdcomp")
            .withColumnRenamed("usg3tp", "rpl_minoritystatus")
            .withColumnRenamed("usg4tp", "rpl_housingtransport")
            .withColumnRenamed("ustp", "svi")
            .withColumnRenamed("_input_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df10 = (spark.read.table("mimi_ws_1.cdc.svi_censustract_y2010")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("r_pl_theme1", "rpl_socioeconomic")
            .withColumnRenamed("r_pl_theme2", "rpl_householdcomp")
            .withColumnRenamed("r_pl_theme3", "rpl_minoritystatus")
            .withColumnRenamed("r_pl_theme4", "rpl_housingtransport")
            .withColumnRenamed("r_pl_themes", "svi")
            .withColumnRenamed("_input_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df14 = (spark.read.table("mimi_ws_1.cdc.svi_censustract_y2014")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport")
            .withColumnRenamed("rpl_themes", "svi")
            .withColumnRenamed("_input_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df16 = (spark.read.table("mimi_ws_1.cdc.svi_censustract_y2016")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport")
            .withColumnRenamed("rpl_themes", "svi")
            .withColumnRenamed("_input_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df18 = (spark.read.table("mimi_ws_1.cdc.svi_censustract_y2018")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport")
            .withColumnRenamed("rpl_themes", "svi")
            .withColumnRenamed("_input_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df20 = (spark.read.table("mimi_ws_1.cdc.svi_censustract_y2020")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport")
            .withColumnRenamed("rpl_themes", "svi")
            .withColumnRenamed("_input_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df22 = (spark.read.table("mimi_ws_1.cdc.svi_censustract_y2022")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumn("county_name", trim(split(col("county"), "County").getItem(0)))
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport")
            .withColumnRenamed("rpl_themes", "svi")
            .withColumnRenamed("_input_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df = (df00.union(df10)
        .union(df14)
        .union(df16)
        .union(df18)
        .union(df20)
        .union(df22))
(df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"mimi_ws_1.cdc.svi_censustract_multiyears"))

# COMMAND ----------


