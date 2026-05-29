# Databricks notebook source
driver_data=[
    (101,"John",4.7),
    (102,"Alice",4.5),
    (103,"David",4.8)
]

driver_columns=[
    "driver_id","driver_name","driver_rating"
]

driver_df=spark.createDataFrame(driver_data,driver_columns)

print(driver_df)

# COMMAND ----------

display(driver_df)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS taxi_lakehouse.silver")

# COMMAND ----------

driver_df.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable("taxi_lakehouse.silver.dim_driver_scd1")

# COMMAND ----------

display(
    spark.table(
        "taxi_lakehouse.silver.dim_driver_scd1"
    )
)

# COMMAND ----------

updated_driver_data=[
    (101,"John",4.9),
    (104,"Emma",4.6)
]

updated_driver_df=spark.createDataFrame(
    updated_driver_data,
    driver_columns
)

# COMMAND ----------

display(updated_driver_df)

# COMMAND ----------

updated_driver_df.createOrReplaceTempView(
    "driver_updates"
)

#creates a table driver_updates and store value there temprowarily in pyspark session memory

# COMMAND ----------

spark.sql("MERGE INTO taxi_lakehouse.silver.dim_driver_scd1 AS TARGET USING driver_updates AS Source ON target.driver_id=Source.driver_id WHEN MATCHED THEN UPDATE SET target.driver_name=source.driver_name,target.driver_rating=source.driver_rating WHEN NOT MATCHED THEN INSERT(driver_id,driver_name,driver_rating) VALUES(source.driver_id,source.driver_name,source.driver_rating)")

# COMMAND ----------

display(
    spark.table(
        "taxi_lakehouse.silver.dim_driver_scd1"
    )
)