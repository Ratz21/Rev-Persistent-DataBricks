# Databricks notebook source
# MAGIC %md
# MAGIC Fetching cleaned silver data

# COMMAND ----------

from pyspark.sql.functions import col, window, avg, min, max, count, sum, when

# 1) Point to the right catalog + schema
spark.sql("USE CATALOG hive_metastore")
spark.sql("USE weather_db")          # this is the schema we created earlier

# 2) Read Silver (qualify with schema just to be safe)
silver_df = spark.table("weather_silver")  # OK after USE weather_db
# or explicitly:
# silver_df = spark.table("weather_db.weather_silver")

display(silver_df.limit(55))



# COMMAND ----------

# MAGIC %md
# MAGIC Gold aggregation

# COMMAND ----------

gold_hourly_df = (
    silver_df
    .groupBy(
        col("station_id"),
        window(col("timestamp"), "1 hour").alias("time_window")
    )
    .agg(
        count("*").alias("records"),
        avg("temperature_c").alias("avg_temp_c"),
        min("temperature_c").alias("min_temp_c"),
        max("temperature_c").alias("max_temp_c"),
        avg("humidity_pct").alias("avg_humidity_pct"),
        avg("wind_speed_kph").alias("avg_wind_speed_kph"),
        max("wind_speed_kph").alias("max_wind_speed_kph"),
        sum(when(col("status") == "OK", 1).otherwise(0)).alias("ok_count"),
        sum(when(col("status") != "OK", 1).otherwise(0)).alias("error_count"),
    )
    .select(
        "station_id",
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        "records",
        "avg_temp_c", "min_temp_c", "max_temp_c",
        "avg_humidity_pct",
        "avg_wind_speed_kph", "max_wind_speed_kph",
        "ok_count", "error_count",
    )
)

display(gold_hourly_df.limit(50))


# COMMAND ----------

# MAGIC %md
# MAGIC Write Gold to Delta

# COMMAND ----------

gold_path = "s3a://weather-stream-raw-ap-south-1/gold/weather_hourly_metrics/"


# COMMAND ----------

(
    gold_hourly_df
    .write
    .mode("overwrite")      # later: "append" for incremental
    .format("delta")
    .save(gold_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Register the Gold table

# COMMAND ----------

spark.sql("USE CATALOG hive_metastore")
# spark.sql("USE weather_db")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS weather_gold_hourly
    USING DELTA
    LOCATION '{gold_path}'
""")


# COMMAND ----------

display(spark.table("weather_gold_hourly").limit(40))


# COMMAND ----------

gold_df = spark.table("weather_gold_hourly")
display(gold_df)
