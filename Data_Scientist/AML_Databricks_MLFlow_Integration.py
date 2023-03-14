# Databricks notebook source
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

workspace_name = "amlsandbox-eco3"
resource_group = "databricks-sandbox-rg"

subscription_id = dbutils.secrets.get(scope="DBX_SP_Credentials",key="SUBSCRIPTION_ID")
DBX_SP_Client_Secret = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret")
DBX_SP_ClientID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_ClientID")
DBX_SP_TenantID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_TenantID")

print(f"Test: {DBX_SP_ClientID}")
print(f"Test: {DBX_SP_Client_Secret}")
print(DBX_SP_TenantID)

os.environ["AZURE_CLIENT_ID"] = DBX_SP_ClientID
os.environ["AZURE_CLIENT_SECRET"] = DBX_SP_Client_Secret
os.environ["AZURE_TENANT_ID"] = DBX_SP_TenantID

# COMMAND ----------

# Use AzureML SDK To Authenticate 

from azureml.core.authentication import ServicePrincipalAuthentication

svc_pr = ServicePrincipalAuthentication(
                        tenant_id=DBX_SP_TenantID,
                        service_principal_id= DBX_SP_ClientID,
                        service_principal_password=DBX_SP_Client_Secret)

ws = Workspace(
        subscription_id=subscription_id,
        resource_group=resource_group,
        workspace_name=workspace_name,
        auth=svc_pr
        )

print(ws)

aml_uri = ws.get_mlflow_tracking_uri()
print(aml_uri)


import mlflow
mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 
print("MLflow tracking URI to point to your Azure ML Workspace setup complete.")

# COMMAND ----------

# Import required libraries
import os
import warnings
import sys

import pandas as pd
import numpy as np
from itertools import cycle
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
from sklearn.linear_model import lasso_path, enet_path
from sklearn import datasets

# Import mlflow
import mlflow
import mlflow.sklearn

# Load diabetes dataset
diabetes = datasets.load_diabetes()
X = diabetes.data
y = diabetes.target

# Create pandas DataFrame 
Y = np.array([y]).transpose()
d = np.concatenate((X, Y), axis=1)
cols = ['age', 'sex', 'bmi', 'bp', 's1', 's2', 's3', 's4', 's5', 's6', 'progression']
data = pd.DataFrame(d, columns=cols)

# COMMAND ----------

def plot_enet_descent_path(X, y, l1_ratio):
    # Compute paths
    eps = 5e-3  # the smaller it is the longer is the path

    # Reference the global image variable
    global image
    
    print("Computing regularization path using ElasticNet.")
    alphas_enet, coefs_enet, _ = enet_path(X, y, eps=eps, l1_ratio=l1_ratio, fit_intercept=False)

    # Display results
    fig = plt.figure(1)
    ax = plt.gca()

    colors = cycle(['b', 'r', 'g', 'c', 'k'])
    neg_log_alphas_enet = -np.log10(alphas_enet)
    for coef_e, c in zip(coefs_enet, colors):
        l1 = plt.plot(neg_log_alphas_enet, coef_e, linestyle='--', c=c)

    plt.xlabel('-Log(alpha)')
    plt.ylabel('coefficients')
    title = 'ElasticNet Path by alpha for l1_ratio = ' + str(l1_ratio)
    plt.title(title)
    plt.axis('tight')

    # Display images
    image = fig
    
    # Save figure
    fig.savefig("ElasticNet-paths.png")

    # Close plot
    plt.close(fig)

    # Return images
    return image  

# COMMAND ----------

# train_diabetes
#   Uses the sklearn Diabetes dataset to predict diabetes progression using ElasticNet
#       The predicted "progression" column is a quantitative measure of disease progression one year after baseline
#       http://scikit-learn.org/stable/modules/generated/sklearn.datasets.load_diabetes.html
def train_diabetes(data, in_alpha, in_l1_ratio):
    # Evaluate metrics
    def eval_metrics(actual, pred):
        rmse = np.sqrt(mean_squared_error(actual, pred))
        mae = mean_absolute_error(actual, pred)
        r2 = r2_score(actual, pred)
        return rmse, mae, r2
    
    warnings.filterwarnings("ignore")
    np.random.seed(40)
    
    # Split the data into training and test sets. (0.75, 0.25) split.
    train, test = train_test_split(data)
    
    # The predicted column is "progression" which is a quantitative measure of disease progression one year after baseline
    train_x = train.drop(["progression"], axis=1)
    test_x = test.drop(["progression"], axis=1)
    train_y = train[["progression"]]
    test_y = test[["progression"]]
    
    if float(in_alpha) is None:
        alpha = 0.05
    else:
        alpha = float(in_alpha)
        
    if float(in_l1_ratio) is None:
        l1_ratio = 0.05
    else:
        l1_ratio = float(in_l1_ratio)
    
    # Start an MLflow run; the "with" keyword ensures we'll close the run even if this cell crashes
    
    
    with mlflow.start_run():
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)
    
        predicted_qualities = lr.predict(test_x)
    
        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)
    
        # Print out ElasticNet model metrics
        print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)
    
        # Log mlflow attributes for mlflow UI
        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)
        mlflow.sklearn.log_model(lr, "model")
        modelpath = "/dbfs/mlflow/test_diabetes/model-%f-%f" % (alpha, l1_ratio)
        mlflow.sklearn.save_model(lr, modelpath)
        
        # Call plot_enet_descent_path
        image = plot_enet_descent_path(X, y, l1_ratio)
        
        # Log artifacts (output files)
        mlflow.log_artifact("ElasticNet-paths.png")

# COMMAND ----------
%fs rm -r dbfs:/mlflow/test_diabetes

# COMMAND ----------

train_diabetes(data, 0.01, 0.01)

# COMMAND ----------

train_diabetes(data, 0.01, 0.75)

# COMMAND ----------

train_diabetes(data, 0.01, 0.01)

# COMMAND ----------


train_diabetes(data, 0.01, .5)

# COMMAND ----------

train_diabetes(data, 0.01, 1)