# Rev-Persistent-DataBricks

Real-Time Weather Streaming Pipeline (AWS + Databricks)


A complete end-to-end real-time data engineering pipeline built using AWS Kinesis, EC2, Firehose, S3, Databricks (Bronze–Silver–Gold model), and Delta Lake.

This project demonstrates cloud-native ingestion, distributed ETL, Delta Lake storage optimization, aggregations, and dashboard-ready Gold tables.



                  ┌──────────────────────────┐
                  │   EC2 Weather Producer   │
                  │ (Python Random Generator)│
                  └──────────────┬───────────┘
                                 │ JSON Events
                                 ▼
                       ┌──────────────────┐
                       │ AWS Kinesis Data │
                       │      Stream      │
                       └─────────┬────────┘
                                 │
                                 ▼
                       ┌──────────────────┐
                       │ Kinesis Firehose │
                       │  (Buffer → S3)   │
                       └─────────┬────────┘
                                 │
                                 ▼
                       ┌──────────────────┐
                       │  S3 Raw Storage  │
                       │  (Partitioned)   │
                       └─────────┬────────┘
                                 │
                  ┌──────────────▼──────────────┐
                  │      Databricks Notebook    │
                  │    Bronze → Silver → Gold   │
                  │ Hive Metastore Delta Tables │
                  └──────────────┬──────────────┘
                                 │
                                 ▼
                       ┌──────────────────┐
                       │   Gold Tables    │
                       │ (Hourly Metrics) │
                       └─────────┬────────┘
                                 │
                                 ▼
                       ┌──────────────────┐
                       │  Dashboard (SQL) │
                       └──────────────────┘


🚀 Features Implemented
✅ Real-time ingestion with AWS Kinesis Stream
✅ Auto-delivery to S3 using Firehose with buffering + prefixing
✅ EC2 Kinesis producer using boto3
✅ Databricks ETL using Bronze → Silver → Gold
✅ Delta Lake format for ACID reliability
✅ Hive Metastore tables for easy SQL access
✅ Visualization



📂 Project Structure
weather-streaming-pipeline/
│
├── producer/
│   ├── producer_kinesis.py         # Python script sending weather events
│   └── README.md
│
├── databricks/
│   ├── 01_bronze_ingestion.py      # Bronze: raw ingest + standardization
│   ├── 02_silver_cleaning.py       # Silver: cleansing + typing
│   ├── 03_gold_aggregation.py      # Gold: hourly weather metrics
│   └── dashboard_sql.txt           # Queries used for dashboard
│
├── README.md                        # Main documentation (this file)
└── architecture.png                 # Optional diagram



1️⃣ Data Ingestion Layer (AWS)
🖥️ EC2 Weather Producer

A Python script runs on EC2 and streams JSON weather events to Kinesis Data Stream every second.

Example event:

{
  "station_id": "ST_3",
  "timestamp": "2025-12-08T17:22:11Z",
  "temperature_c": 29.4,
  "humidity": 62.5,
  "wind_speed": 11.3
}

🔥 Kinesis → Firehose → S3
Firehose settings used
Setting	Value
Source	Kinesis Stream
Buffer Size	1 MiB
Buffer Interval	60 sec
S3 Prefix	kinesis/weather/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/!{timestamp:HH}/
Compression	GZIP
Format	JSON

This creates S3 folders like:

s3://weather-stream-raw-ap-south-1/kinesis/weather/2025/12/08/17/

2️⃣ Bronze Layer – Raw Data Standardization
Why Bronze?

Ingest raw JSON exactly as produced

Add schema, metadata, and recursive ingestion

Store as Delta for ACID and schema evolution

Bronze code:
raw_path = "s3a://weather-stream-raw-ap-south-1/kinesis/weather/*/*/*/*/"

raw_df = (
    spark.read
         .option("recursiveFileLookup", "true")
         .json(raw_path)
)

bronze_df = raw_df.withColumn("ingest_time", current_timestamp())

bronze_df.write.format("delta").mode("overwrite").save(bronze_path)

Bronze Table
hive_metastore.weather_db.weather_bronze

3️⃣ Silver Layer – Cleaning & Transformations
Why Silver?

Fix corrupted or missing values

Convert datatypes

Filter out invalid rows

Standardize timestamp fields

Sample Transformations
silver_df = bronze_df.select(
    col("station_id"),
    to_timestamp("timestamp").alias("event_time"),
    col("temperature_c").cast("double"),
    col("humidity").cast("double"),
    col("wind_speed").cast("double"),
    col("ingest_time")
).dropna()

Silver Table
hive_metastore.weather_db.weather_silver

4️⃣ Gold Layer – Analytics Aggregation
Why Gold?

Business-ready metrics

Aggregations for dashboards

Low-latency reporting tables

Gold performs hourly aggregation per station:

gold_df = (
    silver_df
    .groupBy(
        window("event_time", "1 hour"),
        col("station_id")
    )
    .agg(
        avg("temperature_c").alias("avg_temp_c"),
        min("temperature_c").alias("min_temp_c"),
        max("temperature_c").alias("max_temp_c"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        count("*").alias("row_count")
    )
)

Gold Table
hive_metastore.weather_db.weather_gold_hourly

5️⃣ Dashboard Layer (Databricks SQL)

Example SQL for visualization:

SELECT 
  station_id,
  window_start,
  avg_temp_c,
  avg_humidity,
  avg_wind_speed
FROM weather_db.weather_gold_hourly
ORDER BY window_start DESC;


Charts you can build:

Temperature trend (line chart)

Humidity trend

Station-wise metrics (bar chart)

Wind speed distribution

🧠 Why Hive Metastore Instead of Unity Catalog?

Your workspace did not have Unity Catalog enabled
(which requires account-level setup + storage credentials).

For a single-team POC, hive_metastore is sufficient.

🧱 Why Delta Lake?
Feature	Benefit
ACID Transactions	No partial writes, safe pipeline reruns
Schema Enforcement	Prevent bad JSON records
Time Travel	Debug & rollback
High-performance reads	Used in dashboard queries
Auto-optimized files	Faster aggregations
📊 Business Value Delivered

Real-time weather monitoring

Auto-processing of continuous stream

Clean, consistent analytical dataset

Dashboard-ready aggregated metrics

Demonstrates complete data engineering lifecycle

⚙️ Technologies Used
Component	Technology
Streaming	AWS Kinesis
ETL	Databricks
Storage	S3 + Delta Lake
Metadata	Hive Metastore
Compute	Databricks Cluster
Visualization	Databricks SQL Dashboard
Language	Python + PySpark
📘 Future Enhancements

Migrate tables to Unity Catalog

Add DLT (Delta Live Tables) for managed pipelines

Add Slack Notifications on job failures

Deploy producer using Docker + ECS

Add ML model monitoring weather anomalies

🙌 Acknowledgments

This project was built for hands-on understanding of cloud data engineering and real-time analytics.









                       
