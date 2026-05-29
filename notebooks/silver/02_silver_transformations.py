# Databricks notebook source
taxi_df=spark.table("taxi_lakehouse.bronze.bronze_taxi_datatrips")
weather_df=spark.table("taxi_lakehouse.bronze.bronze_weather")
holiday_df=spark.table("taxi_lakehouse.bronze.bronze_holidays")


# COMMAND ----------

weather_df = weather_df.drop("ingestion_timestamp")
weather_df.columns

# COMMAND ----------

holiday_df=holiday_df.drop("ingestion_timestamp")
holiday_df.columns

# COMMAND ----------

display(taxi_df.limit(10))

# COMMAND ----------

display(weather_df.limit(10))

# COMMAND ----------

display(holiday_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

taxi_df.select([count(when(col(c).isNull(),c)).alias(c) for c in taxi_df.columns]).show()

# COMMAND ----------

taxi_df.filter(
    col("fare_amount") < 0
).show()

# COMMAND ----------

taxi_df.filter(
    col("trip_distance") <= 0
).show()

# COMMAND ----------

clean_taxi_df = taxi_df.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0)
)

# COMMAND ----------

clean_taxi_df = clean_taxi_df.fillna({
    "passenger_count": 1
})

# COMMAND ----------

display(clean_taxi_df.limit(100))

# COMMAND ----------

clean_taxi_df = clean_taxi_df.withColumn(
    "trip_date",
    to_date(col("tpep_pickup_datetime"))
)

# COMMAND ----------

clean_taxi_df = clean_taxi_df.withColumn(
    "trip_weekday",
    date_format(col("tpep_pickup_datetime"), "EEEE")
)

# COMMAND ----------

clean_taxi_df = clean_taxi_df.withColumn(
    "trip_hour",
    hour(col("tpep_pickup_datetime"))
)

# COMMAND ----------

clean_taxi_df=clean_taxi_df.withColumn("is_weekend",when(col("trip_weekday").isin("Saturday","Sunday"),1).otherwise(0))

# COMMAND ----------

clean_taxi_df.groupBy("trip_weekday").count()

# COMMAND ----------

clean_taxi_df = clean_taxi_df.join(
    weather_df,
    clean_taxi_df.trip_date == weather_df.weather_date,
    "left"
)

# COMMAND ----------

display(clean_taxi_df.limit(100))

# COMMAND ----------

clean_taxi_df=clean_taxi_df.join(
    holiday_df,
    clean_taxi_df.trip_date==holiday_df.holiday_date,
    "left"
)

# COMMAND ----------

display(clean_taxi_df.limit(100))

# COMMAND ----------

clean_taxi_df=clean_taxi_df.withColumn(
    "is_holiday",
    when(col("holiday_name").isNotNull(),
         1).otherwise(0)
)

# COMMAND ----------

clean_taxi_df=clean_taxi_df

# COMMAND ----------

total_rows = clean_taxi_df.count()

unique_rows = clean_taxi_df.dropDuplicates().count()

duplicate_count = total_rows - unique_rows

print(f"Total Rows: {total_rows}")
print(f"Unique Rows: {unique_rows}")
print(f"Duplicate Rows: {duplicate_count}")

# COMMAND ----------

clean_taxi_df = clean_taxi_df.dropDuplicates()

# COMMAND ----------

total_rows = clean_taxi_df.count()

unique_rows = clean_taxi_df.dropDuplicates().count()

duplicate_count = total_rows - unique_rows

print(f"Total Rows: {total_rows}")
print(f"Unique Rows: {unique_rows}")
print(f"Duplicate Rows: {duplicate_count}")

# COMMAND ----------

display(clean_taxi_df)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS taxi_lakehouse.silver")

# COMMAND ----------

clean_taxi_df.columns

# COMMAND ----------

clean_taxi_df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("taxi_lakehouse.silver.silver_taxi_cleaned")

# COMMAND ----------

display(
    spark.table(
        "taxi_lakehouse.silver.silver_taxi_cleaned"
    )
)