# Databricks notebook source


#%load_ext autoreload #
#%autoreload 2

# COMMAND ----------

#dbutils.library.restartPython()
# COMMAND ----------
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
from ds_utils import * # Custom Package
import base64
import datetime
from pyspark.dbutils import DBUtils


# COMMAND ----------
# Remove
#import mlflow.azureml
#from pyspark.sql import SparkSession
#from helperFunctions.helperFunction import *
#from helperFunctions.helperFunction import test
#from azure.ai.ml.entities import ManagedOnlineEndpoint, ManagedOnlineDeployment
#from azure.ai.ml import MLClient
#from azure.ai.ml.entities import Model
#from azure.ai.ml.constants import AssetTypes
#import azureml.mlflow
#from azureml.mlflow import get_portal_url
#from mlflow.deployments import get_deploy_client
#from azure.identity import DefaultAzureCredential
#import azureml.core
#import datetime
#from mlflow.models.signature import infer_signature


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


def is_running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def set_up_spark():
    if is_running_in_databricks():
        return SparkSession.builder.getOrCreate()

    from databricks.connect import DatabricksSession
    from databricks.sdk.core import Config
    os.environ["USER"] = "anything"
    config = Config(profile="DEFAULT")

    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    return spark

def load_dot_env_file():
    load_dotenv(".env")

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
            mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri()) 
            mlflow.set_experiment(experiment_name) 
        else:
            mlflow.set_tracking_uri('databricks') 
            experiment_name = "/Shared/" + experiment_name
            mlflow.set_experiment(experiment_name) 
    
    return experiment_name


def load_data(
        spark,
        dbx_dbfs_data_location,
        fs_data_version
        ):
    """
    Loads the data from the given path into a Spark DataFrame

    :param spark: The spark session
    :type spark: SparkSession

    :param data_path: The path to the data
    :type data_path: str

    :param fs_data_version: The version of the data in the feature store
    :type fs_data_version: int

    :returns: The data as a Spark DataFrame
    :rtype: DataFrame

    """
    rounded_unix_timestamp_udf = udf(rounded_unix_timestamp, IntegerType())
    #raw_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")
    
    taxi_data = spark.read.format("delta").option(
        "versionAsOf", 
        fs_data_version
        ).load(
        dbx_dbfs_data_location
        )
    
    taxi_data = rounded_taxi_data(
        spark=spark, 
        taxi_data_df=taxi_data
        )
    
    return taxi_data


def feature_lookup(
        feature_table_name: str,
        feature_lookups: List[FeatureLookup], 
        lookup_key: List[FeatureLookup]
        ):
    """
    Looks up the features from the feature store for the given data
    :param data: The data to lookup features for
    :type data: DataFrame
    :param feature_lookups: The list of FeatureLookups to perform
    :type feature_lookups: List[FeatureLookup]
    :returns: Feature LookUp Object
    :rtype: DataFrame
    """
    feature_lookup = [
        FeatureLookup( 
            table_name = feature_table_name,
            feature_names = feature_lookups,
            lookup_key = lookup_key,
            )
    ]
    return feature_lookup


def get_training_data(
        spark,
        fs,
        data,
        feature_lookups,
        label, 
        exclude_columns
        ):
    """
    Creates a training set from the given data
    :param data: The data to create the training set from
    :type data: DataFrame
    :param feature_lookups: The list of FeatureLookups to perform
    :type feature_lookups: List[FeatureLookup]
    :param label: The label column
    :type label: str
    :param exclude_columns: The columns to exclude from the training set
    :type exclude_columns: List[str]
    :returns: The training set
    :rtype: DataFrame
    """


    training_set = fs.create_training_set(
        data,
        feature_lookups = feature_lookups,
        label = label,
        exclude_columns = exclude_columns
    )
    training_df = training_set.load_df()
    return training_df, training_set


def save_model_dbfs(model, model_file_path):
    joblib.dump(
        model, 
        open(model_file_path,'wb')
    )



# Move This Into Evaluation Script, and Import it as a module
def evaluate(training_df,random_state, model):

    evaluation_dict = {
        "r2": None,
    }

    features_and_label = training_df.columns
    data = training_df.toPandas()[features_and_label]
    train, test = train_test_split(data, random_state=random_state)

    X_test = test.drop(["fare_amount"], axis=1)
    y_test = test.fare_amount

    expected_y  = y_test
    predicted_y = model.predict(X_test)

    from sklearn import metrics
    r2 = metrics.r2_score(
        expected_y, 
        predicted_y
        )

    evaluation_dict["r2"] = r2
    
    mlflow.log_metric(
        "r2",
        r2
    )

    return evaluation_dict



def train_model_lgbm(
    spark,
    training_df,
    training_set,
    fs,
    model_params: Dict[str, str],
    model_name: str,
    model_file_path: str
    ):
    """
    Trains the model
    :param training_df: The training set
    :type training_df: DataFrame
    :param features_and_label: The features and label columns
    :type features_and_label: List[str]
    :param model_type: The type of model to train
    :type model_type: str
    :param model_params: The model parameters
    :type model_params: Dict[str, str]
    :returns: The trained model
    :rtype: PipelineModel
    """

    features_and_label = training_df.columns

    # Collect data into a Pandas array for training
    data = training_df.toPandas()[features_and_label]
    train, test = train_test_split(data, random_state=123)
    
    X_train = train.drop(["fare_amount"], axis=1)
    y_train = train.fare_amount

    mlflow.end_run()
    mlflow.autolog(exclusive=False)
    with mlflow.start_run():
        train_lgb_dataset = lgb.Dataset(
            X_train, 
            label=y_train.values
            )
        
        #test_lgb_dataset = lgb.Dataset(
        #    X_test, 
        #    label=y_test.values
        #    )
        
        num_rounds = model_params["num_rounds"]

        # Train a lightGBM model
        model = lgb.train(
        model_params,
        train_lgb_dataset, 
        num_rounds
        )

        mlflow.log_param("num_rounds", num_rounds)
        #mlflow.log_param("local_model_file_path", model_file_path)  

        evaulation_dict = evaluate(
            training_df=training_df,
            random_state=123,
            model=model
            )
        
        mlflow.log_metrics(evaulation_dict)
    
        fs.log_model(
            model,
            artifact_path="model_packaged",
            flavor=mlflow.lightgbm,
            training_set=training_set,
            registered_model_name=model_name
        )

        return model


def get_model_file_path(
        spark,
        model_folder_name,
        model_name
    ):

    """
    Saves the model to the given path
    :param model: The model to save
    :type model: PipelineModel
    :param model_file_path: The path to save the model to
    :type model_file_path: str
    """

    model_file_path = '/dbfs/' + str(model_folder_name) + '/' + str(model_name)
    return model_file_path

def create_model_folder(
        spark,
        model_folder_name,
    ):
    """
    Creates a folder for the model
    """

    print(is_running_in_databricks())

    if is_running_in_databricks():
        return dbutils.fs.mkdirs("/dbfs/" + model_folder_name)
    else:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        w.dbutils.fs.mkdirs("/dbfs/" + model_folder_name)

    #import pdb; pdb.set_trace()

def save_model_dbfs(model, model_file_path):
    joblib.dump(
        model, 
        open(model_file_path,'wb')
    )


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
        track_in_azure_ml=False
    )


    #data_path
    dbx_dbfs_data_location = "dbfs:/user/hive/warehouse/feature_store_taxi_example.db/nyc_yellow_taxi_with_zips"
    taxi_data = load_data(
        spark,
        dbx_dbfs_data_location=dbx_dbfs_data_location, 
        fs_data_version=0 # First Version Of Data 
        )
    

    fs = feature_store.FeatureStoreClient() # Feature Store Client
    
    pickup_feature_lookups = feature_lookup(
        feature_table_name="feature_store_taxi_example.trip_pickup_features", 
        feature_lookups=["mean_fare_window_1h_pickup_zip", "count_trips_window_1h_pickup_zip"],
        lookup_key=["pickup_zip", "rounded_pickup_datetime"]  
    )

    #import pdb; pdb.set_trace()

    dropoff_feature_lookups = feature_lookup(
        feature_table_name="feature_store_taxi_example.trip_dropoff_features", 
        feature_lookups=["count_trips_window_30m_dropoff_zip", "dropoff_is_weekend"],
        lookup_key=["dropoff_zip", "rounded_dropoff_datetime"]  
        )

    training_df, training_set = get_training_data(
        spark=spark,
        fs=fs,
        data=taxi_data,
        feature_lookups=pickup_feature_lookups + dropoff_feature_lookups,
        label="fare_amount",
        exclude_columns=[
            "rounded_pickup_datetime",
            "rounded_dropoff_datetime"
        ]
    )

    model_folder_name = "cached_models"
    create_model_folder(
        spark=spark,
        model_folder_name=model_folder_name
        )
    
    model_file_path = get_model_file_path(
        spark=spark,
        model_folder_name=model_folder_name,
        model_name="taxi_example_fare_packaged_2"
    )

    model = train_model_lgbm(
        spark=spark,
        model_file_path=model_file_path,
        training_df=training_df, 
        training_set=training_set, # Feature Store Object Prior to df conversion (above)
        fs=fs,
        model_params=model_params,
        model_name=model_name
    )

    latest_model_version = get_latest_model_version(model_name)
    #mlflow.log_param("model_version", latest_model_version)
    mlflow.set_tag("model_version", latest_model_version)


    #save_model_dbfs(model, model_file_path)

# COMMAND ----------

def dbx_execute_functions():
    spark = set_up_spark()

    load_dot_env_file() # Secrets For Local Development
    
    secrets_dict = get_secrets_dict()
    
    ws = get_azure_ml_obj(secrets_dict) # Azure ML Workspace Client
    model_folder_name = "cached_models"
    create_model_folder(
        spark=spark,
        model_folder_name=model_folder_name
        )
    
    get_model_file_path(
        spark=spark,
        model_folder_name=model_folder_name,
        model_name="taxi_example_fare_packaged",
    )
    

# COMMAND ----------

if __name__ == "__main__":
    run_training(
        experiment_name = "ciaran_experiment_nyc_taxi",
        model_name = "taxi_example_fare_packaged",
        model_params = {
            "objective": "regression",
            "metric": "rmse",
            "num_leaves": 32,
            "learning_rate": 0.1,
            "bagging_fraction": 0.9,
            "feature_fraction": 0.9,
            "bagging_seed": 42,
            "verbosity": -1,
            "seed": 42,
            "num_rounds": 100
        }
        )

    #dbx_execute_functions()
# COMMAND ----------

    #model_file_name1 = 'taxi_example_fare_packaged.pkl'
    #model_file_name2 = 'pyfunc_taxi_fare_packaged.pkl'
    #model_file_path1 = os.path.join('/dbfs', MachineLearningExperiment.model_folder, model_file_name1)
    #model_file_path2 = os.path.join('/dbfs', MachineLearningExperiment.model_folder, model_file_name2)
    #pyfunc_model = fareClassifier(model)
    #joblib.dump(pyfunc_model, open(model_file_path2,'wb'))      #Save The Model 

    #mlflow.end_run()
    #mlflow.autolog(exclusive=False)
    #with mlflow.start_run() as run:
    #    fs.log_model(
    #        pyfunc_model,
    #        "pyfunc_packaged_model",
    #        flavor=mlflow.pyfunc,
    #        training_set=training_set,
    #        registered_model_name="pyfunc_taxi_fare_packaged",
    #    )




