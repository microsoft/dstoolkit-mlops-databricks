# Databricks notebook source

# HIGHLY EXPERIMENTAL - DO NOT USE YET


# https://learn.microsoft.com/en-us/azure/databricks/_static/notebooks/mlflow/mlflow-quick-start-deployment-azure.html
# COMMAND ----------
%pip install azureml-mlflow
%pip install azureml-core
%pip install azure-ai-ml
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

model_name = 'test_model'
model_uri = "runs:/d53ec57a17f2482ab298bbd3ee95b484/pyfunc_packaged_model"
try:
    os.mkdir(model_name)
except:
    None


model_path = mlflow.tracking.artifact_utils._download_artifact_from_uri(artifact_uri=model_uri, output_path=model_name)
# COMMAND ----------

workspace_name = "dbxamlws"
resource_group = "databricks-sandbox-rg"

subscription_id = "2a834239-8f89-42e1-8cf1-c3c10090f51c"
tenant_id = "16b3c013-d300-468d-ac64-7eda0820b6d3"

DBX_SP_Client_Secret = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret")
DBX_SP_ClientID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_ClientID")
DBX_SP_TenantID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_TenantID")

print(f"Test: {DBX_SP_ClientID}")
print(f"Test: {DBX_SP_Client_Secret}")
print(DBX_SP_TenantID)

os.environ["AZURE_CLIENT_ID"] = DBX_SP_ClientID
os.environ["AZURE_CLIENT_SECRET"] = DBX_SP_Client_Secret
os.environ["AZURE_TENANT_ID"] = DBX_SP_TenantID


print(model_path)

# COMMAND ----------
mlflow_model = Model(
    path=model_path,
    type=AssetTypes.MLFLOW_MODEL,
    name=model_name,
    description="Model deployed with V2"
)

# COMMAND ----------
azml_client = MLClient(
    DefaultAzureCredential(), subscription_id, resource_group, workspace_name
)

# COMMAND ----------
azml_client.models.create_or_update(mlflow_model)

# COMMAND ----------