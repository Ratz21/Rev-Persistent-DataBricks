# Databricks notebook source
# MAGIC %md
# MAGIC USER ACCES KEY & SECRET KEY SETUP GOT WHEN I CREATE THE ROLE 

# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAWAIXHM4F4JXWM")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "TiFjgkH6etd5AF9gPwvC5NH0OgVKfRapPG")


# COMMAND ----------

# MAGIC %md
# MAGIC DATABRICKS CONNECTION WITH S3

# COMMAND ----------

dbutils.fs.ls("s3a://weather-stream-raw-ap-south-1/kinesis/weather/")


# COMMAND ----------

dbutils.fs.ls("s3a://weather-stream-raw-ap-south-1/kinesis/weather/2025/")
dbutils.fs.ls("s3a://weather-stream-raw-ap-south-1/kinesis/weather/2025/12/")
dbutils.fs.ls("s3a://weather-stream-raw-ap-south-1/kinesis/weather/2025/12/07/")




# COMMAND ----------

BRONZE_PATH = "s3a://weather-stream-raw-ap-south-1/bronze/weather_readings/"
SILVER_PATH = "s3a://weather-stream-raw-ap-south-1/silver/weather_readings/"
GOLD_PATH   = "s3a://weather-stream-raw-ap-south-1/gold/weather_hourly_metrics/"
RAW_PATH    = "s3a://weather-stream-raw-ap-south-1/kinesis/weather/"


# COMMAND ----------

raw_path = "s3a://weather-stream-raw-ap-south-1/kinesis/weather/*/*/*/*/"
raw_df = (
    spark.read
         .option("recursiveFileLookup", "true")
         .json(raw_path)
)

display(raw_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Turn this into a Bronze Delta table ->

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

bronze_df = (
    raw_df
    .withColumn("ingest_ts", current_timestamp())  # when the source data was generated 
)

display(bronze_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Then write it as a Delta table:

# COMMAND ----------

# bronze_path = "dbfs:/weather/bronze/weather_readings"   # Databricks internal storage

# (
#     bronze_df
#     .write
#     .mode("overwrite")          # later switch to append / incremental
#     .format("delta")
#     .save(bronze_path)
# )

# spark.sql("""
#     CREATE TABLE IF NOT EXISTS weather_bronze
#     USING DELTA
#     LOCATION 'dbfs:/weather/bronze/weather_readings'
# """)


# COMMAND ----------

# MAGIC %md
# MAGIC Write Bronze Delta files to S3
# MAGIC
# MAGIC Create a dedicated S3 folder for bronze:

# COMMAND ----------

bronze_path = "s3a://weather-stream-raw-ap-south-1/bronze/weather_readings/"


# COMMAND ----------

# MAGIC %md
# MAGIC # WROTE THE BRONZE DELTA TABLE 

# COMMAND ----------

(
    bronze_df
        .write
        .mode("overwrite")
        .format("delta")
        .save(bronze_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Register the table under hive_metastore

# COMMAND ----------

bronze_path = "s3a://weather-stream-raw-ap-south-1/bronze/weather_readings/"

# 1. Switched to hive_metastore (non-UC) as UC is not available for me to access
spark.sql("USE CATALOG hive_metastore")

# 2. Create a DB to keep things tidy but unity catalog cannot be used so its not letting me implement it so went with hive
# spark.sql("CREATE DATABASE IF NOT EXISTS weather_db")
# spark.sql("USE weather_db")

# 3. Register the external Delta table pointing to your S3 path
spark.sql(f"""
CREATE TABLE IF NOT EXISTS weather_bronze
USING DELTA
LOCATION '{bronze_path}'
""")


# COMMAND ----------

display(spark.table("weather_bronze").limit(40))


# COMMAND ----------

