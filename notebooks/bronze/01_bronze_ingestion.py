# Databricks notebook source
taxi_df = spark.read.parquet(
    "/Volumes/workspace/default/taxi_dataset/"
)


# COMMAND ----------

display(taxi_df)

# COMMAND ----------

taxi_df.printSchema()

# COMMAND ----------

display(taxi_df.limit(10))

# COMMAND ----------

print("Row count :",taxi_df.count())

# COMMAND ----------

from pyspark.sql.functions import *

bronze_df=taxi_df.withColumn("injestion_timestamp",current_timestamp())

# COMMAND ----------

display(bronze_df.limit(10))

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS taxi_lakehouse")

# COMMAND ----------

spark.sql("USE CATALOG taxi_lakehouse")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS BRONZE")

# COMMAND ----------

bronze_df.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable("taxi_lakehouse.bronze.bronze_taxi_datatrips")


# COMMAND ----------

spark.sql("SELECT * FROM taxi_lakehouse.bronze.bronze_taxi_datatrips LIMIT 10")

# COMMAND ----------

display(spark.table("taxi_lakehouse.bronze.bronze_taxi_datatrips").limit(100))

# COMMAND ----------

spark.sql("DESCRIBE HISTORY taxi_lakehouse.bronze.bronze_taxi_datatrips").show(truncate=False)

# COMMAND ----------

dates = spark.sql("""
SELECT explode(sequence(
to_date('2023-01-01'),
to_date('2023-12-31'),
interval 1 day
)) as weather_date
""")

# COMMAND ----------

weather_df = dates.withColumn(
    "temperature",
    (rand()*35 - 5).cast("int")
).withColumn(
    "rain",
    (rand()*20).cast("int")
).withColumn(
    "snowfall",
    when(col("temperature") < 0, rand()*10).otherwise(0)
).withColumn(
    "wind_speed",
    (rand()*40).cast("int")
).withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# COMMAND ----------

display(weather_df)

# COMMAND ----------

weather_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("taxi_lakehouse.bronze.bronze_weather")

# COMMAND ----------

holiday_data = [
    ("2023-01-01", "New Year"),
    ("2023-07-04", "Independence Day"),
    ("2023-11-23", "Thanksgiving"),
    ("2023-12-25", "Christmas")
]

# COMMAND ----------

holiday_df=spark.createDataFrame(holiday_data,["holiday_date","holiday_name"])

# COMMAND ----------

holiday_df = holiday_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# COMMAND ----------

holiday_df.write.format("Delta")\
    .mode("overwrite")\
        .saveAsTable("taxi_lakehouse.bronze.bronze_holidays")


# COMMAND ----------

display(spark.table("taxi_lakehouse.bronze.bronze_holidays"))

# COMMAND ----------

display(spark.table("taxi_lakehouse.bronze.bronze_weather"))

# COMMAND ----------

spark.sql("SHOW TABLES IN taxi_lakehouse.bronze").show()