# Databricks notebook source
silver_df=spark.table("taxi_lakehouse.silver.silver_taxi_cleaned")

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
daily_revenue_df = (
    silver_df
    .groupBy("trip_date")
    .agg(
        sum("fare_amount").alias("daily_revenue")
    )
)

# COMMAND ----------

display(daily_revenue_df)

# COMMAND ----------

hourly_demand_df = (
    silver_df
    .groupBy(
        "trip_date",
        "trip_hour"
    )
    .agg(
        count("*").alias("ride_count")
    )
)

# COMMAND ----------

display(hourly_demand_df)

# COMMAND ----------

zone_perf_df=(
    silver_df
    .groupBy("PULocationID")
    .agg(
        count("*").alias("total_rides"),
        avg("fare_amount").alias("avg_fare"),
        sum("fare_amount").alias("total_revenue")
    )
)

# COMMAND ----------

display(zone_perf_df)

# COMMAND ----------

gold_ml_features_df=(
    silver_df
    .groupBy("trip_date","trip_hour","is_weekend","is_holiday","temperature","rain")
    .agg(count("*").alias("ride_count"))
)

# COMMAND ----------

display(gold_ml_features_df)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS taxi_lakehouse.gold")

# COMMAND ----------

daily_revenue_df.write\
    .mode("overwrite")\
        .format("delta")\
            .saveAsTable("taxi_lakehouse.gold.gold_daily_revenue")

# COMMAND ----------

hourly_demand_df.write\
    .mode("overwrite")\
        .format("delta")\
            .saveAsTable("taxi_lakehouse.gold.gold_hourly_demand")

# COMMAND ----------

zone_perf_df.write\
    .mode("overwrite")\
        .format("delta")\
            .saveAsTable("taxi_lakehouse.gold.gold_zone_performance")

# COMMAND ----------

gold_ml_features_df.write\
    .mode("overwrite")\
        .format("delta")\
            .saveAsTable("taxi_lakehouse.gold.gold_ml_features")

# COMMAND ----------

display(spark.sql("SELECT * FROM taxi_lakehouse.gold.gold_daily_revenue"))

# COMMAND ----------

display(spark.sql("SELECT * FROM taxi_lakehouse.gold.gold_hourly_demand"))

# COMMAND ----------

