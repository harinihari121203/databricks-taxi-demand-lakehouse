# Databricks notebook source
pricing_data=[
    (1,"Manhattan",1.2),
    (2,"Brooklyn",1.0),
    (3,"Queens",0.9)
]

pricing_columns=["zone_id","zone_name","price_multiplier"]

pricing_df=spark.createDataFrame(pricing_data,pricing_columns)

# COMMAND ----------

display(pricing_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

pricing_df=pricing_df.withColumn("effective_date",current_date())

# COMMAND ----------

pricing_df=pricing_df.withColumn("effective_to",lit(None).cast("date"))

# COMMAND ----------

pricing_df=pricing_df.withColumn("is_current",lit(True))

# COMMAND ----------

pricing_df=pricing_df.withColumn("pricing_sk",monotonically_increasing_id())

# COMMAND ----------

display(pricing_df)

# COMMAND ----------

pricing_df.write.format("delta")\
    .mode("Overwrite")\
        .saveAsTable("taxi_lakehouse.silver.dim_zone_pricing_scd2")

# COMMAND ----------

updated_pricing_data=[
    (1,"Manhattan",1.5),
    (3,"Queens",1.1)
]

updated_pricing_df=spark.createDataFrame(updated_pricing_data,pricing_columns)

# COMMAND ----------

display(updated_pricing_df)

# COMMAND ----------

updated_pricing_df.createOrReplaceTempView(
    "pricing_updates"
)

# COMMAND ----------

spark.sql("UPDATE taxi_lakehouse.silver.dim_zone_pricing_scd2 SET effective_to=current_date(),is_current=FALSE WHERE zone_id in (SELECT zone_id from pricing_updates) AND is_current=True ")


# COMMAND ----------

display(spark.sql("SELECT * FROM taxi_lakehouse.silver.dim_zone_pricing_scd2"))

# COMMAND ----------

new_version_df=updated_pricing_df\
    .withColumn("effective_date",current_date()) \
        .withColumn("effective_to",lit(None).cast("date")) \
            .withColumn("is_current",lit(True))\
                .withColumn("pricing_sk",monotonically_increasing_id())

# COMMAND ----------

display(new_version_df)

# COMMAND ----------


new_version_df.write.format("delta")\
    .mode("append")\
    .saveAsTable("taxi_lakehouse.silver.dim_zone_pricing_scd2")

# COMMAND ----------

display(spark.sql("SELECT * FROM taxi_lakehouse.silver.dim_zone_pricing_scd2"))

# COMMAND ----------

updated_value=[(1,"Manhattan",2.8)]

updated_df=spark.createDataFrame(updated_value,pricing_columns)
display(updated_df)
updated_df.createOrReplaceTempView("pricing_updates")
spark.sql("UPDATE taxi_lakehouse.silver.dim_zone_pricing_scd2 SET price_multiplier=2.8 WHERE zone_id in (SELECT zone_id from pricing_updates) AND is_current=True")
display(spark.sql("SELECT * FROM taxi_lakehouse.silver.dim_zone_pricing_scd2"))

# COMMAND ----------

