# Databricks notebook source
from databricks.sdk.runtime import *
import os
from sklearn import metrics
import joblib
from pyspark.sql import *
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType, IntegerType, StringType
from pyspark.sql.functions import *
import mlflow.pyfunc
from databricks.feature_store import feature_table, FeatureLookup
from databricks import feature_store
import mlflow
from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient
import lightgbm as lgb
import mlflow.lightgbm
import yaml
import pathlib
import sys
from argparse import ArgumentParser
import mlflow
from azureml.core import Workspace
import os
from azureml.core.authentication import ServicePrincipalAuthentication
from dotenv import load_dotenv
from common import * # Custom Package
import base64
import datetime
from pyspark.dbutils import DBUtils

# COMMAND ----------

import mlflow
import mlflow.pyfunc
import mlflow.sklearn
import numpy as np
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from mlflow.models.signature import infer_signature
from mlflow.utils.environment import _mlflow_conda_env
import cloudpickle
import time
 
# The predict method of sklearn's RandomForestClassifier returns a binary classification (0 or 1). 
# The following code creates a wrapper function, SklearnModelWrapper, that uses 
# the predict_proba method to return the probability that the observation belongs to each class. 
 
class SklearnModelWrapper(mlflow.pyfunc.PythonModel):
  def __init__(self, model):
    self.model = model
    
  def predict(self, context, model_input):
    return self.model.predict_proba(model_input)[:,1]
 

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Header
# MAGIC

# COMMAND ----------

import yaml
import mlflow 
import pathlib

def set_mlflow(
    namespace,
    ws,
    experiment_name,
    track_in_azure_ml=False
    ):
    if namespace.env is not None:
        params = yaml.safe_load(pathlib.Path(namespace.env).read_text())
        experiment_name = params['Global']['ExperimentName']
        track_in_azure_ml = params['Global']['AMLTraking']

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
            print(ws.get_mlflow_tracking_uri())
            mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 
            mlflow.set_experiment(experiment_name) 
        else:
            mlflow.set_tracking_uri('databricks') 
            experiment_name = "/Shared/" + experiment_name
            mlflow.set_experiment(experiment_name) 
    
    return experiment_name

# COMMAND ----------

def get_azure_ml_obj(
    secrets_dict: Dict[str, str]
    ):
    
    """
    Gets the Azure ML workspace object for interacting with
    Azure ML resources

    :param tenant_id: The tenant ID
    :type: tenant_id: str

    :param service_principal_id: The service principal ID
    :type: service_principal_id: str

    :param service_principal_password: The service principal password
    :type: service_principal_password: str

    :param subscription_id: The subscription ID
    :type: subscription_id: str

    :param resource_group: The resource group
    :type: resource_group: str

    :param workspace_name: The workspace name
    :type: workspace_name: str

    :param workspace_region: The workspace region
    :type: workspace_region: str
        
    :returns: Initialised Workspace object
    :rtype: Workspace
    """
    
    svc_pr = ServicePrincipalAuthentication(
        tenant_id=secrets_dict['tenant_id'],
        service_principal_id=secrets_dict['service_principal_id'],
        service_principal_password=secrets_dict['service_principal_password']
    )

    return Workspace(
        subscription_id=secrets_dict['subscription_id'],
        resource_group=secrets_dict['resource_group'],
        workspace_name=secrets_dict['workspace_name'],
        auth=svc_pr      
    )


# COMMAND ----------

def get_secrets_dict():
    """
    Gets the secrets from the Databricks secrets store
    
    :param w: The Workspace object
    :type w: Workspace
    
    :returns: The secrets as a dictionary
    :rtype: Dict[str, str]

    """

    
    secrets_dict = {
        "subscription_id": get_secret("DBX_SP_Credentials", "SUBSCRIPTION_ID"),
        "service_principal_id": get_secret("DBX_SP_Credentials", "DBX_SP_Client_ID"),
        "tenant_id": get_secret("DBX_SP_Credentials", "DBX_SP_Tenant_ID"),
        "service_principal_password": get_secret("DBX_SP_Credentials", "DBX_SP_Client_Secret"),
        "resource_group": get_secret("AzureResourceSecrets", "RESOURCE_GROUP_NAME"),
        "workspace_name": get_secret("AzureResourceSecrets", "AML_WS_NAME")
    }
    
    return secrets_dict

# COMMAND ----------

def set_up_spark():
    if is_running_in_databricks():
        return SparkSession.builder.getOrCreate()

    from databricks.connect import DatabricksSession
    from databricks.sdk.core import Config
    os.environ["USER"] = "anything"
    config = Config(profile="DEFAULT")

    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    return spark

# COMMAND ----------

def get_extenal_parameters():
    """
    Parameters Can Be Passed From An External CLI / Job
    They Are Stored In The Namespace Object

    :returns: The external_parameters Object

    """
    p = ArgumentParser()
    p.add_argument("--env", required=False, type=str)
    
    external_parameters = p.parse_known_args(sys.argv[1:])[0]
    
    return external_parameters

# COMMAND ----------

external_parameters = get_extenal_parameters()
print(external_parameters)

# COMMAND ----------


def run_training(
    experiment_name,
    model_name,
    model_params,
    ):

    external_parameters = get_extenal_parameters()
    
    spark = set_up_spark()

    load_dot_env_file() # Secrets For Local Development
    
    secrets_dict = get_secrets_dict()
    
    ws = get_azure_ml_obj(secrets_dict) # Azure ML Workspace Client
    
    set_mlflow(
        external_parameters,
        ws,
        experiment_name,
        track_in_azure_ml=True
    )

    import pandas as pd
 

    ############
    white_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-white.csv", sep=";")
    red_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-red.csv", sep=";")


    red_wine['is_red'] = 1
    white_wine['is_red'] = 0
    
    data = pd.concat([red_wine, white_wine], axis=0)
    
    # Remove spaces from column names
    data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

    print(data)
    #import seaborn as sns
    #sns.distplot(data.quality, kde=False)

    high_quality = (data.quality >= 7).astype(int)
    data.quality = high_quality

    import matplotlib.pyplot as plt
 
    dims = (3, 4)
 
    f, axes = plt.subplots(dims[0], dims[1], figsize=(25, 15))
    axis_i, axis_j = 0, 0
    for col in data.columns:
        if col == 'is_red' or col == 'quality':
            continue # Box plots cannot be used on indicator variables
    #sns.boxplot(x=high_quality, y=data[col], ax=axes[axis_i, axis_j])
    axis_j += 1
    if axis_j == dims[1]:
        axis_i += 1
        axis_j = 0

    from sklearn.model_selection import train_test_split
    
    X = data.drop(["quality"], axis=1)
    y = data.quality
    
    # Split out the training data
    X_train, X_rem, y_train, y_rem = train_test_split(X, y, train_size=0.6, random_state=123)
    
    # Split the remaining data equally into validation and test
    X_val, X_test, y_val, y_test = train_test_split(X_rem, y_rem, test_size=0.5, random_state=123)



    # mlflow.start_run creates a new MLflow run to track the performance of this model. 
    # Within the context, you call mlflow.log_param to keep track of the parameters used, and
    # mlflow.log_metric to record metrics like accuracy.
    mlflow.end_run()
    mlflow.autolog(exclusive=False)
    with mlflow.start_run(run_name='untuned_random_forest'):
        n_estimators = 10
        model = RandomForestClassifier(n_estimators=n_estimators, random_state=np.random.RandomState(123))
        model.fit(X_train, y_train)
        
        # predict_proba returns [prob_negative, prob_positive], so slice the output with [:, 1]
        predictions_test = model.predict_proba(X_test)[:,1]
        auc_score = roc_auc_score(y_test, predictions_test)
        mlflow.log_param('n_estimators', n_estimators)
        # Use the area under the ROC curve as a metric.
        mlflow.log_metric('auc', auc_score)
        wrappedModel = SklearnModelWrapper(model)
        # Log the model with a signature that defines the schema of the model's inputs and outputs. 
        # When the model is deployed, this signature will be used to validate inputs.
        signature = infer_signature(X_train, wrappedModel.predict(None, X_train))
        
        # MLflow contains utilities to create a conda environment used to serve models.
        # The necessary dependencies are added to a conda.yaml file which is logged along with the model.
        conda_env =  _mlflow_conda_env(
                additional_conda_deps=None,
                additional_pip_deps=["cloudpickle=={}".format(cloudpickle.__version__), "scikit-learn=={}".format(sklearn.__version__)],
                additional_conda_channels=None,
            )
        mlflow.pyfunc.log_model("random_forest_model", python_model=wrappedModel, conda_env=conda_env, signature=signature)
        run_id = mlflow.active_run().info.run_id
        model_name = "random_forest_model"
        artifact_path = "random_forest_model"
        model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=artifact_path)
        model_details = mlflow.register_model(model_uri=model_uri, name=model_name)






    # #data_path
    # dbx_dbfs_data_location = "dbfs:/user/hive/warehouse/feature_store_taxi_example.db/nyc_yellow_taxi_with_zips"
    # taxi_data = load_data(
    #     spark,
    #     dbx_dbfs_data_location=dbx_dbfs_data_location, 
    #     fs_data_version=0 # First Version Of Data 
    #     )
    

    # fs = feature_store.FeatureStoreClient() # Feature Store Client
    
    # pickup_feature_lookups = feature_lookup(
    #     feature_table_name="feature_store_taxi_example.trip_pickup_features", 
    #     feature_lookups=["mean_fare_window_1h_pickup_zip", "count_trips_window_1h_pickup_zip"],
    #     lookup_key=["pickup_zip", "rounded_pickup_datetime"]  
    # )

    # #import pdb; pdb.set_trace()

    # dropoff_feature_lookups = feature_lookup(
    #     feature_table_name="feature_store_taxi_example.trip_dropoff_features", 
    #     feature_lookups=["count_trips_window_30m_dropoff_zip", "dropoff_is_weekend"],
    #     lookup_key=["dropoff_zip", "rounded_dropoff_datetime"]  
    #     )

    # training_df, training_set = get_training_data(
    #     spark=spark,
    #     fs=fs,
    #     data=taxi_data,
    #     feature_lookups=pickup_feature_lookups + dropoff_feature_lookups,
    #     label="fare_amount",
    #     exclude_columns=[
    #         "rounded_pickup_datetime",
    #         "rounded_dropoff_datetime"
    #     ]
    # )

    # model_folder_name = "cached_models"
    # create_model_folder(
    #     spark=spark,
    #     model_folder_name=model_folder_name
    #     )
    
    # model_file_path = get_model_file_path(
    #     spark=spark,
    #     model_folder_name=model_folder_name,
    #     model_name="taxi_example_fare_packaged_2"
    # )

    # model = train_model_lgbm(
    #     spark=spark,
    #     model_file_path=model_file_path,
    #     training_df=training_df, 
    #     training_set=training_set, # Feature Store Object Prior to df conversion (above)
    #     fs=fs,
    #     model_params=model_params,
    #     model_name=model_name
    # )

    # latest_model_version = get_latest_model_version(model_name)
    # #mlflow.log_param("model_version", latest_model_version)
    # mlflow.set_tag("model_version", latest_model_version)


    # #save_model_dbfs(model, model_file_path)


# COMMAND ----------

def is_running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

# COMMAND ----------

def load_dot_env_file():
    load_dotenv(".env")

# COMMAND ----------

def get_secret(scope: str, secret_name: str) -> str:
    """
    Gets a secret from the Databricks secrets store from the
    given scope as described by the secret name.
    This does not work using databricks-connect remotely.

    :param: scope: The secrets scope in which the secret is stored
    :type scope: str
    
    :param: secret_name: the name of the secret to retrieve
    :type secret_name: str
    
    :returns: The secret as a string
    :rtype: str
    
    """

    if os.environ.get(secret_name):
        return os.environ.get(secret_name)
    
    from pyspark.dbutils import DBUtils
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    return dbutils.secrets.get(scope=scope, key=secret_name)

def databricks_sdk_secrets(scope, secret_name):
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient() # Databricks Workspace Client
    w.dbutils.secrets.get(scope, secret_name)

# COMMAND ----------

run_training(
    experiment_name = "wine_experiment",
    model_name = "wine_quality_model",
    model_params = {
        "objective": "regression",
        "metric": "rmse",
        "num_leaves": 25,
        "learning_rate": 0.2,
        "bagging_fraction": 0.9,
        "feature_fraction": 0.9,
        "bagging_seed": 42,
        "verbosity": -1,
        "seed": 42,
        "num_rounds": 100
    }
)


# COMMAND ----------

#run_registration(
#    model_name = "taxi_example_fare_packaged"
#)
