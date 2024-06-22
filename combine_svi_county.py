# Databricks notebook source
# MAGIC %md
# MAGIC # Combine the multi-year SVI files
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit, split, to_date, trim, when

# COMMAND ----------

#.withColumn("state_name", col("state_name"))
#.withColumn("state_abbr", col("state_abbr"))
df00 = (spark.read.table("mimi_ws_1.cdc.svi_county_y2000")
            .withColumn("county_name", trim(col("county")))
            .withColumnRenamed("stcofips", "fips")
            .withColumnRenamed("usg1tp", "rpl_socioeconomic_")
            .withColumnRenamed("usg2tp", "rpl_householdcomp_")
            .withColumnRenamed("usg3tp", "rpl_minoritystatus_")
            .withColumnRenamed("usg4tp", "rpl_housingtransport_")
            .withColumnRenamed("ustp", "svi_")
            .withColumn("rpl_socioeconomic", 
                        when(col("rpl_socioeconomic_") < 0, lit(None)).otherwise(col("rpl_socioeconomic_")))
            .withColumn("rpl_householdcomp", 
                        when(col("rpl_householdcomp_") < 0, lit(None)).otherwise(col("rpl_householdcomp_")))
            .withColumn("rpl_minoritystatus", 
                        when(col("rpl_minoritystatus_") < 0, lit(None)).otherwise(col("rpl_minoritystatus_")))
            .withColumn("rpl_housingtransport", 
                        when(col("rpl_housingtransport_") < 0, lit(None)).otherwise(col("rpl_housingtransport_")))
            .withColumn("svi", 
                        when(col("svi_") < 0, lit(None)).otherwise(col("svi_")))
            .withColumnRenamed("mimi_src_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df10 = (spark.read.table("mimi_ws_1.cdc.svi_county_y2010")
            .withColumnRenamed("st", "state_abbr")
            .withColumn("county_name", trim(split("location", " County,").getItem(0)))
            .withColumnRenamed("r_pl_theme1", "rpl_socioeconomic_")
            .withColumnRenamed("r_pl_theme2", "rpl_householdcomp_")
            .withColumnRenamed("r_pl_theme3", "rpl_minoritystatus_")
            .withColumnRenamed("r_pl_theme4", "rpl_housingtransport_")
            .withColumnRenamed("r_pl_themes", "svi_")
            .withColumn("rpl_socioeconomic", 
                        when(col("rpl_socioeconomic_") < 0, lit(None)).otherwise(col("rpl_socioeconomic_")))
            .withColumn("rpl_householdcomp", 
                        when(col("rpl_householdcomp_") < 0, lit(None)).otherwise(col("rpl_householdcomp_")))
            .withColumn("rpl_minoritystatus", 
                        when(col("rpl_minoritystatus_") < 0, lit(None)).otherwise(col("rpl_minoritystatus_")))
            .withColumn("rpl_housingtransport", 
                        when(col("rpl_housingtransport_") < 0, lit(None)).otherwise(col("rpl_housingtransport_")))
            .withColumn("svi", 
                        when(col("svi_") < 0, lit(None)).otherwise(col("svi_")))
            .withColumnRenamed("mimi_src_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df14 = (spark.read.table("mimi_ws_1.cdc.svi_county_y2014")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic_")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp_")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus_")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport_")
            .withColumnRenamed("rpl_themes", "svi_")
            .withColumn("rpl_socioeconomic", 
                        when(col("rpl_socioeconomic_") < 0, lit(None)).otherwise(col("rpl_socioeconomic_")))
            .withColumn("rpl_householdcomp", 
                        when(col("rpl_householdcomp_") < 0, lit(None)).otherwise(col("rpl_householdcomp_")))
            .withColumn("rpl_minoritystatus", 
                        when(col("rpl_minoritystatus_") < 0, lit(None)).otherwise(col("rpl_minoritystatus_")))
            .withColumn("rpl_housingtransport", 
                        when(col("rpl_housingtransport_") < 0, lit(None)).otherwise(col("rpl_housingtransport_")))
            .withColumn("svi", 
                        when(col("svi_") < 0, lit(None)).otherwise(col("svi_")))
            .withColumnRenamed("mimi_src_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df16 = (spark.read.table("mimi_ws_1.cdc.svi_county_y2016")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic_")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp_")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus_")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport_")
            .withColumnRenamed("rpl_themes", "svi_")
            .withColumn("rpl_socioeconomic", 
                        when(col("rpl_socioeconomic_") < 0, lit(None)).otherwise(col("rpl_socioeconomic_")))
            .withColumn("rpl_householdcomp", 
                        when(col("rpl_householdcomp_") < 0, lit(None)).otherwise(col("rpl_householdcomp_")))
            .withColumn("rpl_minoritystatus", 
                        when(col("rpl_minoritystatus_") < 0, lit(None)).otherwise(col("rpl_minoritystatus_")))
            .withColumn("rpl_housingtransport", 
                        when(col("rpl_housingtransport_") < 0, lit(None)).otherwise(col("rpl_housingtransport_")))
            .withColumn("svi", 
                        when(col("svi_") < 0, lit(None)).otherwise(col("svi_")))
            .withColumnRenamed("mimi_src_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df18 = (spark.read.table("mimi_ws_1.cdc.svi_county_y2018")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic_")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp_")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus_")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport_")
            .withColumnRenamed("rpl_themes", "svi_")
            .withColumn("rpl_socioeconomic", 
                        when(col("rpl_socioeconomic_") < 0, lit(None)).otherwise(col("rpl_socioeconomic_")))
            .withColumn("rpl_householdcomp", 
                        when(col("rpl_householdcomp_") < 0, lit(None)).otherwise(col("rpl_householdcomp_")))
            .withColumn("rpl_minoritystatus", 
                        when(col("rpl_minoritystatus_") < 0, lit(None)).otherwise(col("rpl_minoritystatus_")))
            .withColumn("rpl_housingtransport", 
                        when(col("rpl_housingtransport_") < 0, lit(None)).otherwise(col("rpl_housingtransport_")))
            .withColumn("svi", 
                        when(col("svi_") < 0, lit(None)).otherwise(col("svi_")))
            .withColumnRenamed("mimi_src_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df20 = (spark.read.table("mimi_ws_1.cdc.svi_county_y2020")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumnRenamed("county", "county_name")
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic_")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp_")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus_")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport_")
            .withColumnRenamed("rpl_themes", "svi_")
            .withColumn("rpl_socioeconomic", 
                        when(col("rpl_socioeconomic_") < 0, lit(None)).otherwise(col("rpl_socioeconomic_")))
            .withColumn("rpl_householdcomp", 
                        when(col("rpl_householdcomp_") < 0, lit(None)).otherwise(col("rpl_householdcomp_")))
            .withColumn("rpl_minoritystatus", 
                        when(col("rpl_minoritystatus_") < 0, lit(None)).otherwise(col("rpl_minoritystatus_")))
            .withColumn("rpl_housingtransport", 
                        when(col("rpl_housingtransport_") < 0, lit(None)).otherwise(col("rpl_housingtransport_")))
            .withColumn("svi", 
                        when(col("svi_") < 0, lit(None)).otherwise(col("svi_")))
            .withColumnRenamed("mimi_src_file_date", "year")
            .select("state_abbr", "county_name", "fips",
                    "rpl_socioeconomic",
                    "rpl_householdcomp",
                    "rpl_minoritystatus",
                    "rpl_housingtransport",
                    "svi", "year"))

# COMMAND ----------

df22 = (spark.read.table("mimi_ws_1.cdc.svi_county_y2022")
            .withColumnRenamed("st_abbr", "state_abbr")
            .withColumn("county_name", trim(split(col("county"), "County").getItem(0)))
            .withColumnRenamed("rpl_theme1", "rpl_socioeconomic_")
            .withColumnRenamed("rpl_theme2", "rpl_householdcomp_")
            .withColumnRenamed("rpl_theme3", "rpl_minoritystatus_")
            .withColumnRenamed("rpl_theme4", "rpl_housingtransport_")
            .withColumnRenamed("rpl_themes", "svi_")
            .withColumn("rpl_socioeconomic", 
                        when(col("rpl_socioeconomic_") < 0, lit(None)).otherwise(col("rpl_socioeconomic_")))
            .withColumn("rpl_householdcomp", 
                        when(col("rpl_householdcomp_") < 0, lit(None)).otherwise(col("rpl_householdcomp_")))
            .withColumn("rpl_minoritystatus", 
                        when(col("rpl_minoritystatus_") < 0, lit(None)).otherwise(col("rpl_minoritystatus_")))
            .withColumn("rpl_housingtransport", 
                        when(col("rpl_housingtransport_") < 0, lit(None)).otherwise(col("rpl_housingtransport_")))
            .withColumn("svi", 
                        when(col("svi_") < 0, lit(None)).otherwise(col("svi_")))
            .withColumnRenamed("mimi_src_file_date", "year")
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
        .saveAsTable(f"mimi_ws_1.cdc.svi_county_multiyears"))

# COMMAND ----------


