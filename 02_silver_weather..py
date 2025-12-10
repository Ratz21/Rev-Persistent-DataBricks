# Databricks notebook source
# MAGIC %md
# MAGIC Set the correct catalog + schema
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql("USE hive_metastore.weather_db")



# COMMAND ----------

bronze_df = spark.table("weather_bronze")
display(bronze_df.limit(70))


# COMMAND ----------

# spark.sql("USE weather_db")  # not needed if we fully qualify

bronze_df = spark.table("hive_metastore.weather_db.weather_bronze")
display(bronze_df.limit(74))


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = (
    bronze_df
    .withColumn("temperature_c", F.col("temperature_c").cast("float"))
    .withColumn("humidity_pct",   F.col("humidity_pct").cast("float"))
    .withColumn("wind_speed_kph", F.col("wind_speed_kph").cast("float"))
    .withColumn("event_time", F.col("timestamp").cast("timestamp"))
    .filter(F.col("status") == "OK")
    .dropDuplicates(["station_id", "event_time"])
)

display(silver_df.limit(78))


# COMMAND ----------

# MAGIC %md
# MAGIC Write Silver table to Delta (cleaned weather data)

# COMMAND ----------

silver_path = "s3a://weather-stream-raw-ap-south-1/silver/weather_cleaned"

(
    silver_df.write
        .mode("overwrite")
        .format("delta")
        .save(silver_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Register the Silver table 

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS weather_silver
USING DELTA
LOCATION 's3a://weather-stream-raw-ap-south-1/silver/weather_cleaned'
""")


# COMMAND ----------

display(spark.table("weather_silver").limit(100))


# COMMAND ----------

