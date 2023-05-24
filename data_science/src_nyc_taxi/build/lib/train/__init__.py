# Databricks notebook source
# Modules.

# If Deploying To MLFlow in Azure ML: mlflow.setexperiment must be a single experiment name "ciaran_experiment"
# Contrast this with Databricks which requires a foder structure --> shared/ciaranex

# Install python helper function wheel file (in dist) to cluster 
# Install pypi packages azureml-sdk[databricks], lightgbm, uszipcode
# The above will be automated in due course 


# https://learn.microsoft.com/en-us/azure/databricks/_extras/notebooks/source/machine-learning/automl-feature-store-example.html

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType, IntegerType, StringType
from pyspark.sql.functions import *
import mlflow.pyfunc
from databricks.feature_store import FeatureLookup
import mlflow
from helperFunctions.helperFunction import *
#from helperFunctions.helperFunction import test
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
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


# COMMAND ----------

# MAGIC %md ## Ingest Args (If Triggered From Pipeline)

# COMMAND ----------

p = ArgumentParser()
p.add_argument("--env", required=False, type=str)
namespace = p.parse_known_args(sys.argv[1:])[0]
display(namespace)
print(namespace)

# COMMAND ----------

# MAGIC %md ## Set Azure ML Configs

# COMMAND ----------

class SparkRunner:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        #self.is_running_locally = self.check_running_locally()
    
    def get_dbutils(self) -> DBUtils:
        """
        Gets the databricks utilities object for interacting with
        Databricks artifacts such as the DBFS
        :returns: Initialised DBUtils object
        :rtype: DBUtils
        """
        return DBUtils(self.spark)

    def get_secret(self, scope: str, secret_name: str) -> str:
        """
        Gets a secret from the Databricks secrets store from the
        given scope as described by the secret name.
        This does not work using databricks-connect remotely.

        :param scope: The secrets scope in which the secret is stored
        :type scope: str
        :param secret_name: the name of the secret to retrieve
        :type secret_name: str
        :returns: The secret as a string
        :rtype: str
        """
        if os.environ.get(secret_name):
            from dotenv import load_dotenv
            load_dotenv(".env")
            return os.environ.get(secret_name)
        dbutils = self.get_dbutils()
        return dbutils.secrets.get(scope, secret_name)

# COMMAND ----------

# MAGIC %md ## Set Azure ML Configs

# COMMAND ----------

class AzureMLConfiguration:
    def __init__ (self, spark: SparkSession,
                  tenant_id: str,
                  subscription_id: str,
                  resource_group: str,
                  workspace_name: str,
                  workspace_region: str,
                  service_principal_id: str,
                  service_principal_password: str
                  ):
        self.tennat_id = tenant_id
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.workspace_name = workspace_name
        self.workspace_region = workspace_region
        self.service_principal_id = service_principal_id
        self.service_principal_password = service_principal_password
        self.spark = spark

    def get_workspace_auth(self) -> Workspace:
        """
        Gets the Azure ML workspace object for interacting with
        Azure ML resources
        :returns: Initialised Workspace object
        :rtype: Workspace
        """
        svc_pr = ServicePrincipalAuthentication(
            tenant_id=self.tennat_id,
            service_principal_id=self.service_principal_id,
            service_principal_password=self.service_principal_password
        )
        return Workspace(
            subscription_id=self.subscription_id,
            resource_group=self.resource_group,
            workspace_name=self.workspace_name,
            auth=svc_pr
        )

# COMMAND ----------


class MachineLearningExperiment:
    def __init__(self, spark: SparkSession, experiment_name: str, namespace: str, workspace: Workspace):
        self.spark = spark
        self.experiment_name = experiment_name
        self.track_in_azure_ml = False
        self.namespace = namespace
        self.ws = workspace
        self.model_folder = "cached_models"
        self.dbutils = SparkRunner().get_dbutils()


    def set_experiment(self) -> mlflow.tracking.MlflowClient:
        if self.namespace.env is not None:
            params = yaml.safe_load(pathlib.Path(namespace.env).read_text())
            experiment_name = params['ML_PIPELINE_FILES']['TRAIN_REGISTER']['PARAMETERS']['EXPERIMENT_NAME']
            track_in_azure_ml = params['ML_PIPELINE_FILES']['TRAIN_REGISTER']['PARAMETERS']['TRACK_IN_AZURE_ML']

            if track_in_azure_ml:
                if track_in_azure_ml: 
                    mlflow.set_tracking_uri(self.ws.get_mlflow_tracking_uri()) 
                    mlflow.set_experiment(experiment_name) 
            else:
                mlflow.set_tracking_uri('databricks') 
                experiment_name = "/Shared/" + experiment_name
                mlflow.set_experiment(experiment_name) 
        else:
            if self.track_in_azure_ml: 
                mlflow.set_tracking_uri(self.ws.get_mlflow_tracking_uri()) 
                mlflow.set_experiment(experiment_name) 
            else:
                mlflow.set_tracking_uri('databricks') 
                experiment_name = "/Shared/" + self.experiment_name
                mlflow.set_experiment(experiment_name) 
        print(experiment_name)
    
    def load_data(self, spark, data_path, fs_data_version) -> DataFrame:
        """
        Loads the data from the given path into a Spark DataFrame
        :param data_path: The path to the data
        :type data_path: str
        :returns: The data as a Spark DataFrame
        :rtype: DataFrame
        """
        rounded_unix_timestamp_udf = udf(rounded_unix_timestamp, IntegerType())
        #raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
        print(spark)
        #raw_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips") # "feature_store_taxi_example.nyc_yellow_taxi_with_zips" 
        raw_data = spark.read.format("delta").option("versionAsOf", fs_data_version).load("dbfs:/user/hive/warehouse/feature_store_taxi_example.db/nyc_yellow_taxi_with_zips")

        taxi_data = rounded_taxi_data(raw_data)
        #display(taxi_data)
        return taxi_data
    

    def feature_lookup(self, feature_table_name: str, feature_lookups: List[FeatureLookup], lookup_key: List[FeatureLookup]) -> DataFrame:
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
    
    def create_model_folder(self):
        """
        Creates a folder for the model
        """
        self.dbutils.fs.mkdirs(self.model_folder)


    def get_model_file_path(self, model_name: str) -> str:
        """
        Saves the model to the given path
        :param model: The model to save
        :type model: PipelineModel
        :param model_file_path: The path to save the model to
        :type model_file_path: str
        """

        model_file_path = os.path.join('/dbfs', self.model_folder, model_name)
        
        return model_file_path


    def get_taining_data(self, fs, data, feature_lookups, label, exclude_columns):
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



    def train_model(
        self,
        training_df,
        training_set,
        fs,
        model_params: Dict[str, str],
        model_name: str
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

        from sklearn import metrics
        import joblib
        features_and_label = training_df.columns

        # Collect data into a Pandas array for training
        data = training_df.toPandas()[features_and_label]
        train, test = train_test_split(data, random_state=123)
        X_train = train.drop(["fare_amount"], axis=1)
        X_test = test.drop(["fare_amount"], axis=1)
        y_train = train.fare_amount
        y_test = test.fare_amount


        mlflow.end_run()
        mlflow.autolog(exclusive=False)
        with mlflow.start_run():
            #mlflow.lightgbm.autolog()
            train_lgb_dataset = lgb.Dataset(
                X_train, 
                label=y_train.values
                )
            
            test_lgb_dataset = lgb.Dataset(
                X_test, 
                label=y_test.values
                )
            
            mlflow.log_param("num_leaves", "32")
            mlflow.log_param("objective", "regression")
            mlflow.log_param( "metric", "rmse")
            mlflow.log_param("learn_rate", "100")

            param = { 
                        "num_leaves": 32, 
                        "objective": "regression", 
                        "metric": "rmse"
                    }
            num_rounds = 100

            # Train a lightGBM model
            model = lgb.train(
            param, 
            train_lgb_dataset, 
            num_rounds
            )
            
            #Save The Model  

            self.create_model_folder()

            model_file_path = self.get_model_file_path("taxi_example_fare_packaged")
            print(f"ModelFilePath: {model_file_path}")
            joblib.dump(
                model, 
                open(model_file_path,'wb')
            )
            mlflow.log_param("local_model_file_path", model_file_path)  

            expected_y  = y_test
            predicted_y = model.predict(X_test)

            r2 = metrics.r2_score(
                expected_y, 
                predicted_y
                )

            mlflow.log_metric(
                "r2",
                r2)
            
            fs.log_model(
                model,
                artifact_path="model_packaged",
                flavor=mlflow.lightgbm,
                training_set=training_set,
                registered_model_name=model_name
            )

            return model
              

# COMMAND ----------

#raw_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")

# COMMAND ----------


def main():
    experiment_name = "ciaran_experiment_nyc_taxi"
    spark_obj = SparkRunner()
    spark = spark_obj.spark
    print(spark)
    subscription_id = spark_obj.get_secret(
        "DBX_SP_Credentials",
        "SUBSCRIPTION_ID"
        )
    resource_group = spark_obj.get_secret(
        "AzureResourceSecrets",
        "RESOURCE_GROUP_NAME"
        )
    workspace_name = spark_obj.get_secret(
        "AzureResourceSecrets", 
        "AML_WS_NAME"
        )
    tenant_id = spark_obj.get_secret(
        "DBX_SP_Credentials",
        "DBX_SP_Tenant_ID"
        )
    service_principal_id = spark_obj.get_secret(
        "DBX_SP_Credentials",
        "DBX_SP_Client_ID"
        )
    service_principal_password = spark_obj.get_secret(
        "DBX_SP_Credentials",
        "DBX_SP_Client_Secret"
        )

    azure_ml_obj = AzureMLConfiguration(
        spark=spark,
        tenant_id=tenant_id,
        service_principal_id=service_principal_id,
        service_principal_password=service_principal_password,
        subscription_id=subscription_id,
        resource_group=resource_group,
        workspace_name=workspace_name,
        workspace_region="uksouth"
        )
    ws = azure_ml_obj.get_workspace_auth()

    ml_ex_obj = MachineLearningExperiment(
        spark=spark,
        experiment_name=experiment_name,
        namespace=namespace,
        workspace=ws
        )


    
    ml_ex_obj.set_experiment()

    # Provide The Data Version of The Feature Store
    # This Allows Model Development Whilst Ensuring The Underlying Data Is Not Changed Downstream By Feature Enginers / Data Engineers
    fs_data_version = 0
    data_path = "dbfs:/user/hive/warehouse/feature_store_taxi_example.db/nyc_yellow_taxi_with_zips"
    
    taxi_data = ml_ex_obj.load_data(spark=spark, data_path=data_path, fs_data_version=fs_data_version)
    
    #taxi_data = ml_ex_obj.load_data(spark=spark, data_path="feature_store_taxi_example.nyc_yellow_taxi_with_zips", fs_data_version=fs_data_version)
    # dbfs:/user/hive/warehouse/feature_store_taxi_example.db/nyc_yellow_taxi_with_zips


    pickup_feature_lookups = ml_ex_obj.feature_lookup(
        feature_table_name="feature_store_taxi_example.trip_pickup_features", 
        feature_lookups=["mean_fare_window_1h_pickup_zip", "count_trips_window_1h_pickup_zip"],
        lookup_key=["pickup_zip", "rounded_pickup_datetime"]  
    )

    dropoff_feature_lookups = ml_ex_obj.feature_lookup(
        feature_table_name="feature_store_taxi_example.trip_dropoff_features", 
        feature_lookups=["count_trips_window_30m_dropoff_zip", "dropoff_is_weekend"],
        lookup_key=["dropoff_zip", "rounded_dropoff_datetime"]  
        )
    
    fs = feature_store.FeatureStoreClient()

    training_df, training_set = ml_ex_obj.get_taining_data(
        fs,
        taxi_data,
        pickup_feature_lookups + dropoff_feature_lookups,
        label="fare_amount",
         exclude_columns=[
            "rounded_pickup_datetime",
            "rounded_dropoff_datetime"
        ]
    )


    print(fs)

    model1 = ml_ex_obj.train_model(
        training_df, 
        training_set, # Feature Store Object Prior to df conversion (above)
        fs,
        model_params={
            "num_leaves": 32,
            "objective": "regression",
            "metric": "rmse"
        },
        model_name="taxi_example_fare_packaged"
    )
    
    
if __name__ == "__main__":
    main()




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





