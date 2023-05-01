# Databricks notebook source


# COMMAND ----------

# MAGIC %pip install databricks-feature-store 
# MAGIC %pip install lightgbm

# COMMAND ----------

dbutils.library.restartPython()

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

# COMMAND ----------

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

# COMMAND ----------

import os
from azureml.core.authentication import ServicePrincipalAuthentication

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

p = ArgumentParser()
p.add_argument("--env", required=False, type=str)
namespace = p.parse_known_args(sys.argv[1:])[0]
display(namespace)

# COMMAND ----------

# Build this out with three options
# 1. If it is run locally (else)
# 2. If it is being deployed from Databricks Job Cluster (namespace.env is not None )
# 3. If it is being deployed from AML Pipeline (to do more)

if namespace.env is not None:
    display(namespace.env)
    params = yaml.safe_load(pathlib.Path(namespace.env).read_text())
    display(params)
    experiment_name = params['ML_PIPELINE_FILES']['MODEL_TRAINING']['PARAMETERS']['EXPERIMENT_NAME']
    display(experiment_name)
    mlflow.set_experiment(experiment_name=experiment_name) 

else:
    display("Set The Parameters Manually, As We Are Deploying From UI")
    mlflow.set_experiment("/Shared/experiments/ciaran_ex") 

# COMMAND ----------

mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 

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

import os

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

mlflow.end_run()
mlflow.start_run() 
mlflow.autolog(exclusive=False)
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

import joblib

joblib.dump(model, open(model_file_path1,'wb'))      #Save The Model 
mlflow.end_run()

# COMMAND ----------

#Register/Log The Model (Should this be done after scoring ?)
fs.log_model(
  model,
  artifact_path="model_packaged",
  flavor=mlflow.lightgbm,
  training_set=training_set,
  registered_model_name="taxi_example_fare_packaged"
)

# COMMAND ----------



# COMMAND ----------

pyfunc_model = fareClassifier(model)
joblib.dump(pyfunc_model, open(model_file_path2,'wb'))      #Save The Model 

# End the current MLflow run and start a new one to log the new pyfunc model
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

model = mlflow.pyfunc.load_model(model_file_path2)

# COMMAND ----------

# Set Up AML MLFlow 

#workspace_name = "amlsandbox-eco3"
#resource_group = "databricks-sandbox-rg"

#subscription_id = dbutils.secrets.get(scope="DBX_SP_Credentials",key="SUBSCRIPTION_ID")
#DBX_SP_Client_Secret = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret")
#DBX_SP_ClientID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_ClientID")
#DBX_SP_TenantID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_TenantID")

#print(f"Test: {DBX_SP_ClientID}")
#print(f"Test: {DBX_SP_Client_Secret}")
#print(DBX_SP_TenantID)

#os.environ["AZURE_CLIENT_ID"] = DBX_SP_ClientID
#os.environ["AZURE_CLIENT_SECRET"] = DBX_SP_Client_Secret
#os.environ["AZURE_TENANT_ID"] = DBX_SP_TenantID

# COMMAND ----------

# Use AzureML SDK To Authenticate. 

#from azureml.core.authentication import ServicePrincipalAuthentication

#svc_pr = ServicePrincipalAuthentication(
#                        tenant_id=DBX_SP_TenantID,
#                        service_principal_id= DBX_SP_ClientID,
#                        service_principal_password=DBX_SP_Client_Secret)

#ws = Workspace(
#        subscription_id=subscription_id,
#        resource_group=resource_group,
#        workspace_name=workspace_name,
#        auth=svc_pr
#        )

#print(ws)

#aml_uri = ws.get_mlflow_tracking_uri()
#print(aml_uri)


#import mlflow
#mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 
#print("MLflow tracking URI to point to your Azure ML Workspace setup complete.")

#mlflow.end_run()
#mlflow.set_experiment("/Shared/dbxDevelopment") 
#with mlflow.start_run():
    # Log mlflow attributes for mlflow UI
#    mlflow.log_param("alpha", "test")
#    mlflow.log_param("l1_ratio", "random")
#    mlflow.log_metric("rmse", 3)
#    mlflow.sklearn.log_model(pyfunc_model, "model")

# COMMAND ----------

#mlflow.sklearn.save_model(lr, "model.pkl")
#from azureml.core import Model
#model = Model.register(workspace=ws, 
#    model_name='nyc-taxi-fare',
#    model_path='model.pkl', # local path
#    description='Model to predict taxi fares in NYC.')

# COMMAND ----------


