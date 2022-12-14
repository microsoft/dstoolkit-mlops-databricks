# Databricks notebook source

# COMMAND ----------

# Modules.

from pyspark.sql import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType, IntegerType, StringType
from pyspark.sql.functions import *
import mlflow.pyfunc
from databricks.feature_store import FeatureLookup
import mlflow
from helperFunctions.helperFunction import *
from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient
import lightgbm as lgb
import mlflow.lightgbm
from mlflow.models.signature import infer_signature
import yaml
import pathlib
import sys
from argparse import ArgumentParser

p = ArgumentParser()
p.add_argument("--env", required=False, type=str)
namespace = p.parse_known_args(sys.argv[1:])[0]
display(namespace)

# COMMAND ----------
if namespace.env is not None:
    display(namespace.env)
    params = yaml.safe_load(pathlib.Path(namespace.env).read_text())
    display(params)
    experiment_name = params['ML_PIPELINE_FILES']['MODEL_TRAINING']['PARAMETERS']['EXPERIMENT_NAME']
    display(experiment_name)
    mlflow.set_experiment(experiment_name=experiment_name) 

else:
    display("Set The Parameters Manually, As We Are Deploying From UI")
    mlflow.set_experiment("/Shared/dbxDevelopment") 


# COMMAND ----------
rounded_unix_timestamp_udf = udf(rounded_unix_timestamp, IntegerType())
raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
taxi_data = rounded_taxi_data(raw_data)
display(taxi_data)

# COMMAND ----------
pickup_features_table = "feature_store_taxi_example.trip_pickup_features"
dropoff_features_table = "feature_store_taxi_example.trip_dropoff_features"

pickup_feature_lookups = [
   FeatureLookup( 
     table_name = pickup_features_table,
     feature_names = ["mean_fare_window_1h_pickup_zip", "count_trips_window_1h_pickup_zip"],
     lookup_key = ["pickup_zip", "rounded_pickup_datetime"],
   ),
]

dropoff_feature_lookups = [
   FeatureLookup( 
     table_name = dropoff_features_table,
     feature_names = ["count_trips_window_30m_dropoff_zip", "dropoff_is_weekend"],
     lookup_key = ["dropoff_zip", "rounded_dropoff_datetime"],
   ),
]


# COMMAND ----------
mlflow.end_run()
mlflow.start_run() 
exclude_columns = ["rounded_pickup_datetime", "rounded_dropoff_datetime"]

fs = feature_store.FeatureStoreClient()
training_set = fs.create_training_set(
  taxi_data,
  feature_lookups = pickup_feature_lookups + dropoff_feature_lookups,
  label = "fare_amount",
  exclude_columns = exclude_columns
)

training_df = training_set.load_df()

display(training_df)


# COMMAND ----------

features_and_label = training_df.columns

# Collect data into a Pandas array for training
data = training_df.toPandas()[features_and_label]

train, test = train_test_split(data, random_state=123)
X_train = train.drop(["fare_amount"], axis=1)
X_test = test.drop(["fare_amount"], axis=1)
y_train = train.fare_amount
y_test = test.fare_amount

mlflow.lightgbm.autolog()
train_lgb_dataset = lgb.Dataset(X_train, label=y_train.values)
test_lgb_dataset = lgb.Dataset(X_test, label=y_test.values)

param = {"num_leaves": 32, "objective": "regression", "metric": "rmse"}
num_rounds = 100

# Train a lightGBM model
model = lgb.train(
  param, train_lgb_dataset, num_rounds
)


# COMMAND ----------
fs.log_model(
  model,
  artifact_path="model_packaged",
  flavor=mlflow.lightgbm,
  training_set=training_set,
  registered_model_name="taxi_example_fare_packaged"
)

# COMMAND ----------
pyfunc_model = fareClassifier(model)

# End the current MLflow run and start a new one to log the new pyfunc model
mlflow.end_run()

with mlflow.start_run() as run:
  fs.log_model(
      pyfunc_model,
      "pyfunc_packaged_model",
      flavor=mlflow.pyfunc,
      training_set=training_set,
      registered_model_name="pyfunc_taxi_fare_packaged",
  )

# COMMAND ----------
