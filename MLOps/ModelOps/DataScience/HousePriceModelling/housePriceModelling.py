# Databricks notebook source

# COMMAND ----------
import os
import numpy as np
import pandas as pd
import pickle
import sklearn
import joblib
import math

from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn_pandas import DataFrameMapper
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

import matplotlib
import matplotlib.pyplot as plt

import azureml
from azureml.core import Workspace, Experiment, Run
from azureml.core.model import Model

print('The azureml.core version is {}'.format(azureml.core.VERSION))

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

import pandas as pd
from sklearn.datasets import fetch_california_housing

california_housing = fetch_california_housing()
pd_df_california_housing = pd.DataFrame(california_housing.data, columns = california_housing.feature_names) 
pd_df_california_housing['target'] = pd.Series(california_housing.target)

# COMMAND ----------
import mlflow
mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 
aml_uri = ws.get_mlflow_tracking_uri()
print(aml_uri)
#mlflow.set_tracking_uri("databricks") 
mlflow.set_experiment("/Shared/houseprice_modeling_Experiment")

# COMMAND ----------

print("Training model...")
output_folder = 'outputs'
model_file_name = 'california-housing.pkl'
code_file_name = "ModelBuilding"

dbutils.fs.mkdirs(output_folder)
model_file_path = os.path.join('/dbfs', output_folder, model_file_name)

code_file_path = os.path.join('/dbfs', output_folder, code_file_name)
print(code_file_path)

# COMMAND ----------
mlflow.autolog(exclusive=False)
with mlflow.start_run(run_name="exp-adb-aml-connection") as run:            #Start MLFlow Tracking
    df = pd_df_california_housing.dropna()  #Drop Missing Values
    x_df = df.drop(['target'], axis=1)      #Create Feature Set 
    y_df = df['target']                     #Create Label

    X_train, X_test, y_train, y_test = train_test_split(x_df, y_df, test_size=0.2, random_state=0) #Train-Test Split

    numerical = ['MedInc', 'HouseAge', 'AveRooms', 'AveBedrms', 'Population', 'AveOccup','Latitude','Longitude']    #List all Numerical Features

    numeric_transformations = [([f], Pipeline(steps=[
      ('imputer', SimpleImputer(strategy='median')),                                         #Imputation and Scaling for all numerical variables.
      ('scaler', StandardScaler())])) for f in numerical]                            

    transformations = numeric_transformations

    clf = Pipeline(steps=[('preprocessor', DataFrameMapper(transformations, df_out=True)),    #Define the pipeline and its steps
                         ('regressor', GradientBoostingRegressor())])

    clf.fit(X_train, y_train)                         #Fit the Model

    y_predict = clf.predict(X_test)                   #Predict on the test data
    y_actual = y_test.values.flatten().tolist()    

    rmse = math.sqrt(mean_squared_error(y_actual, y_predict))   #Calculate and log RMSE
    mlflow.log_metric('rmse', rmse)
    mae = mean_absolute_error(y_actual, y_predict)    #Calculate and log MAE
    mlflow.log_metric('mae', mae)
    r2 = r2_score(y_actual, y_predict)                #Calculate and log R2 Score
    mlflow.log_metric('R2 score', r2)

    plt.figure(figsize=(10,10))
    plt.scatter(y_actual, y_predict, c='crimson')     #Scatter Plot
    plt.yscale('log')
    plt.xscale('log')

    p1 = max(max(y_predict), max(y_actual))
    p2 = min(min(y_predict), min(y_actual))           #Plot regression line
    plt.plot([p1, p2], [p1, p2], 'b-')
    plt.xlabel('True Values', fontsize=15)
    plt.ylabel('Predictions', fontsize=15)
    plt.axis('equal')
    results_graph = os.path.join('/dbfs', output_folder, 'results.png') #Log the graph plotted above
    plt.savefig(results_graph)
    mlflow.log_artifact(results_graph)

    joblib.dump(clf, open(model_file_path,'wb'))      #Log the Model
    mlflow.log_artifact(model_file_path)
    

    run_id = run.info.run_id
    #run_id = mlflow.active_run()
    mlflow.log_param('databricks_runid', run_id)
    print(run_id)

# COMMAND ----------

import mlflow
print("MLflow tracking URI to point to your Databricks Workspace setup complete.")
mlflow.set_tracking_uri("databricks") 
#mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 
#aml_uri = ws.get_mlflow_tracking_uri()
#print(aml_uri)

# Ensure You Have RBAC Set
experiment_name = 'exp-adb-aml-connection'
mlflow.set_experiment(experiment_name)
print("Experiment setup complete.")

# COMMAND ----------

with mlflow.start_run(run_name="exp-adb-aml-connection") as run:            #Start MLFlow Tracking
    mlflow.log_metric('rmse', rmse)
    mlflow.log_metric('mae', mae)
    mlflow.log_metric('R2 score', r2)
    mlflow.log_artifact(results_graph)
    mlflow.log_artifact(model_file_path)     #Log the Model
    mlflow.log_param('databricks_runid', run_id)






    
    
    
    



