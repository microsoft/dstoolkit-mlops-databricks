# Databricks notebook source
# COMMAND ----------
# Modules.

# If Deploying To MLFlow in Azure ML: mlflow.setexperiment must be a single experiment name "ciaran_experiment"
# Contrast this with Databricks which requires a foder structure --> shared/ciaranex

# Install python helper function wheel file (in dist) to cluster 
# Install pypi packages azureml-sdk[databricks], lightgbm, uszipcode
# The above will be automated in due course 

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
import mlflow
import mlflow.azureml
import azureml.mlflow
import azureml.core
from azureml.core import Workspace
from azureml.mlflow import get_portal_url
from mlflow.deployments import get_deploy_client
from azure.identity import DefaultAzureCredential
import os
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Model
from azure.ai.ml.constants import AssetTypes
import datetime
from azure.ai.ml.entities import ManagedOnlineEndpoint, ManagedOnlineDeployment
from azureml.core.authentication import ServicePrincipalAuthentication

# COMMAND ----------

%md
## Ingest Args (If Triggered From Pipeline)

# COMMAND ----------

p = ArgumentParser()
p.add_argument("--env", required=False, type=str)
namespace = p.parse_known_args(sys.argv[1:])[0]
display(namespace)

# COMMAND ----------

%md

## Set Azure ML Configs

# COMMAND ----------

#Provide the Subscription ID of your existing Azure subscription
subscription_id = dbutils.secrets.get(scope="DBX_SP_Credentials",key="SUBSCRIPTION_ID"),

#Replace the name below with the name of your resource group
resource_group = dbutils.secrets.get(scope="AzureResourceSecrets",key="RESOURCE_GROUP_NAME")

#Replace the name below with the name of your Azure Machine Learning workspace
workspace_name = dbutils.secrets.get(scope="AzureResourceSecrets",key="AML_WS_NAME")

print(subscription_id)
print(resource_group)
print(workspace_name)

svc_pr = ServicePrincipalAuthentication(
                        tenant_id = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Tenant_ID"),
                        service_principal_id = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_ID"),
                        service_principal_password = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret") )


ws = Workspace(
        subscription_id="2a834239-8f89-42e1-8cf1-c3c10090f51c",
        resource_group=resource_group,
        workspace_name=workspace_name,
        auth=svc_pr
        )

# COMMAND ----------

%md

## Set MLFlow Tracking Server

# COMMAND ----------

experiment_name = "ciaran_experiment"
track_in_azure_ml = False

if namespace.env is not None:
    params = yaml.safe_load(pathlib.Path(namespace.env).read_text())
    experiment_name = params['ML_PIPELINE_FILES']['MODEL_TRAINING']['PARAMETERS']['EXPERIMENT_NAME']
    track_in_azure_ml = params['ML_PIPELINE_FILES']['MODEL_TRAINING']['PARAMETERS']['TRACK_IN_AZURE_ML']

    if track_in_azure_ml:
        if track_in_azure_ml: 
            mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 
            mlflow.set_experiment(experiment_name) 
    else:
        mlflow.set_tracking_uri('databricks') 
        experiment_name = "/Shared/" + experiment_name
        mlflow.set_experiment(experiment_name) 
else:
    if track_in_azure_ml: 
        mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 
        mlflow.set_experiment(experiment_name) 
    else:
        mlflow.set_tracking_uri('databricks') 
        experiment_name = "/Shared/" + experiment_name
        mlflow.set_experiment(experiment_name) 
print(experiment_name)

# COMMAND ----------

rounded_unix_timestamp_udf = udf(rounded_unix_timestamp, IntegerType())
#raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
raw_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")
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
output_folder = 'outputs'
model_file_name1 = 'taxi_example_fare_packaged.pkl'
model_file_name2 = 'pyfunc_taxi_fare_packaged.pkl'
code_file_name = "ModelBuilding"

dbutils.fs.mkdirs(output_folder)
model_file_path1 = os.path.join('/dbfs', output_folder, model_file_name1)
model_file_path2 = os.path.join('/dbfs', output_folder, model_file_name2)

code_file_path = os.path.join('/dbfs', output_folder, code_file_name)
print(code_file_path)

# COMMAND ----------

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

from sklearn import metrics

features_and_label = training_df.columns

# Collect data into a Pandas array for training
data = training_df.toPandas()[features_and_label]
train, test = train_test_split(data, random_state=123)
X_train = train.drop(["fare_amount"], axis=1)
X_test = test.drop(["fare_amount"], axis=1)
y_train = train.fare_amount
y_test = test.fare_amount



import joblib

mlflow.end_run()
mlflow.autolog(exclusive=False)
with mlflow.start_run():
    #mlflow.lightgbm.autolog()
    train_lgb_dataset = lgb.Dataset(X_train, label=y_train.values)
    test_lgb_dataset = lgb.Dataset(X_test, label=y_test.values)
    
    mlflow.log_param("num_leaves", "32")
    mlflow.log_param("objective", "regression")
    mlflow.log_param( "metric", "rmse")
    mlflow.log_param("learn_rate", "100")

    param = {"num_leaves": 32, "objective": "regression", "metric": "rmse"}
    num_rounds = 100

    # Train a lightGBM model
    model = lgb.train(
    param, train_lgb_dataset, num_rounds
    )

    joblib.dump(model, open(model_file_path1,'wb'))      #Save The Model 

    expected_y  = y_test
    predicted_y = model.predict(X_test)

    r2 = metrics.r2_score(expected_y, predicted_y)
    mlflow.log_metric("r2", r2)
    print(r2)
    
    fs.log_model(
    model,
    artifact_path="model_packaged",
    flavor=mlflow.lightgbm,
    training_set=training_set,
    registered_model_name="taxi_example_fare_packaged"
    )

# COMMAND ----------

pyfunc_model = fareClassifier(model)
joblib.dump(pyfunc_model, open(model_file_path2,'wb'))      #Save The Model 

mlflow.end_run()
mlflow.autolog(exclusive=False)
with mlflow.start_run() as run:
    fs.log_model(
        pyfunc_model,
        "pyfunc_packaged_model",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name="pyfunc_taxi_fare_packaged",
    )

# COMMAND ----------








