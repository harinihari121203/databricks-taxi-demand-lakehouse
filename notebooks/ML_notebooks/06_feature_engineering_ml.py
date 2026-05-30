# Databricks notebook source
df = spark.table(
    "taxi_lakehouse.gold.gold_ml_features"
)

# COMMAND ----------

display(df)

# COMMAND ----------

feature_df = df.select(
    "trip_hour",
    "is_weekend",
    "is_holiday",
    "temperature",
    "rain",
    "ride_count"
)

# COMMAND ----------

train_df, test_df = feature_df.randomSplit(
    [0.8, 0.2],
    seed=42
)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

assembler = VectorAssembler(
    inputCols=[
        "trip_hour",
        "is_weekend",
        "is_holiday",
        "temperature",
        "rain"
    ],
    outputCol="features",
    handleInvalid="skip"
)

# COMMAND ----------

train_data = assembler.transform(train_df)
test_data = assembler.transform(test_df)

# COMMAND ----------

display(
    train_data.filter(train_data.features.isNotNull()).select(
        "features",
        "ride_count"
    )
)

# COMMAND ----------

train_data.count()

# COMMAND ----------

test_data.count()

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor

rf=RandomForestRegressor(
    featuresCol="features",
    labelCol="ride_count"
)

model=rf.fit(train_data)

# COMMAND ----------

predictions = model.transform(test_data)

# COMMAND ----------

display(
    predictions.select(
        "ride_count",
        "prediction"
    )
)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluvator = RegressionEvaluator(
    labelCol="ride_count",
    predictionCol="prediction",
    metricName="rmse"
)   

rmse=evaluvator.evaluate(predictions)

print("RMSE =",rmse)

# COMMAND ----------

from pyspark.sql.functions import min, max, avg

feature_df.select(
    min("ride_count"),
    max("ride_count"),
    avg("ride_count")
).show()