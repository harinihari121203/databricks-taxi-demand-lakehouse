# Databricks notebook source
import mlflow
import mlflow.spark

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


# COMMAND ----------

df=spark.table("taxi_lakehouse.gold.gold_ml_features")

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

feature_df=df.select(
    "trip_hour",
    "is_weekend",
    "is_holiday",
    "temperature",
    "rain",
    "ride_count"
)


train_df,test_df=feature_df.randomSplit(
    [0.8,0.2],
    seed=42
)

# COMMAND ----------

assembler=VectorAssembler(
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

train_data=assembler.transform(train_df)
test_data=assembler.transform(test_df)

# COMMAND ----------

evaluator = RegressionEvaluator(
    labelCol="ride_count",
    predictionCol="prediction",
    metricName="rmse"
)

# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN taxi_lakehouse").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS taxi_lakehouse.ml;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS taxi_lakehouse.ml.mlflow_artifacts;

# COMMAND ----------

from mlflow.models.signature import infer_signature
import pandas as pd
train_pdf = train_sample.toPandas()

# IMPORTANT: flatten vector into list
train_pdf["features"] = train_pdf["features"].apply(lambda x: x.toArray().tolist())
pred_pdf = sample_predictions.select("prediction").toPandas()
signature = infer_signature(train_pdf, pred_pdf)

# COMMAND ----------

input_example = {
    "features": train_pdf["features"].iloc[0]
}

# COMMAND ----------

with mlflow.start_run():

    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="ride_count",
        numTrees=100,
        maxDepth=8
    )

    model = rf.fit(train_data)

    predictions = model.transform(test_data)

    rmse = evaluator.evaluate(predictions)

    mlflow.log_param(
        "numTrees",
        100
    )

    mlflow.log_param(
        "maxDepth",
        8
    )

    mlflow.log_metric(
        "rmse",
        rmse
    )

    mlflow.spark.log_model(
    model,
    artifact_path="random_forest_model",
    dfs_tmpdir="/Volumes/taxi_lakehouse/ml/mlflow_artifacts",
    signature=signature,
    input_example=input_example
)

    print("RMSE =", rmse)
    run_id = mlflow.active_run().info.run_id

# COMMAND ----------

# MAGIC %md
# MAGIC MODEL REGISTERATION
# MAGIC

# COMMAND ----------

import mlflow

model_uri = f"runs:/{run_id}/random_forest_model"

mlflow.register_model(
    model_uri=model_uri,
    name="taxi_ride_rf_model"
)

# COMMAND ----------

client.set_registered_model_alias(
    name="workspace.default.taxi_ride_rf_model",
    alias="Production",
    version=1
)

# COMMAND ----------

model = mlflow.spark.load_model(
    "models:/workspace.default.taxi_ride_rf_model@Production",
    dfs_tmpdir="/Volumes/taxi_lakehouse/ml/mlflow_artifacts"
)

# COMMAND ----------

# DBTITLE 1,Create Model Serving Endpoint
# Create a model serving endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

# Configuration
endpoint_name = "taxi-ride-prediction-endpoint"

try:
    # Create served entity configuration
    served_entity = ServedEntityInput(
        entity_name="workspace.default.taxi_ride_rf_model",
        entity_version="1",
        workload_size="Small",
        scale_to_zero_enabled=True
    )
    
    # Endpoint config
    config = EndpointCoreConfigInput(
        served_entities=[served_entity]
    )
    
    # Create the endpoint
    print(f"Creating endpoint '{endpoint_name}'...")
    print("This will take 5-10 minutes. ⏳")
    
    endpoint = w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=config
    )
    
    print(f"\n✓ Endpoint created successfully!")
    print(f"\nEndpoint name: {endpoint.name}")
    print(f"State: {endpoint.state}")
    print(f"\nYou can now make predictions using:")
    print(f"  Endpoint: /serving-endpoints/{endpoint_name}/invocations")
    
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"✓ Endpoint '{endpoint_name}' already exists!")
        print(f"\nView it at: Serving → {endpoint_name}")
    else:
        print(f"❌ Error: {e}")
        print("\n📌 Alternative: Create via UI")
        print("  1. Go to 'Serving' in the left sidebar")
        print("  2. Click 'Create serving endpoint'")
        print(f"  3. Name: {endpoint_name}")
        print("  4. Model: workspace.default.taxi_ride_rf_model@Production")
        print("  5. Workload size: Small")
        print("  6. Click 'Create'")