# Databricks notebook source


# COMMAND ----------

# Install python helper function wheel file (in dist) to cluster 
# Install pypi packages azureml-sdk[databricks], lightgbm, uszipcode
# The above will be automated in due course 

from databricks import feature_store
from pyspark.sql.types import *
from helperFunctions.helperFunction import *
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
import mlflow
from mlflow.tracking import MlflowClient

def wait_until_ready(model_name, model_version, client):
  for _ in range(10):
    model_version_details = client.get_model_version(
      name=model_name,
      version=model_version,
    )
    status = ModelVersionStatus.from_string(model_version_details.status)
    print("Model status: %s" % ModelVersionStatus.to_string(status))
    if status == ModelVersionStatus.READY:
      break
    time.sleep(1)



def evaluation(fs, taxi_data, model_name, model, training_set, run_id, client):
    taxi_data = rounded_taxi_data(taxi_data)

    cols = ['fare_amount', 'trip_distance', 'pickup_zip', 'dropoff_zip', 'rounded_pickup_datetime', 'rounded_dropoff_datetime']
    taxi_data_reordered = taxi_data.select(cols)
    display(taxi_data_reordered)


    # If no model currently exists in production stage, simply register the model, and promote it the production stage
    model_stage = "production"
    model_uri = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage)
    
    if not model_uri:

        # MOVE TO REGISTRATION #############################################################################################

        #artifact_path = "model"
        #model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=artifact_path)
        
        latest_model_version = get_latest_model_version(model_name)
        model_uri = f"models:/taxi_example_fare_packaged/{latest_model_version}"

        model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

        # wait until the reigstered model is ready
        wait_until_ready(model_details.name, model_details.version, client)

        client.update_registered_model(
        name=model_details.name,
        description="Insert"
        )

        client.update_model_version(
        name=model_details.name,
        version=model_details.version,
        description="Insert"
        )
        #############################################################################################################################

    else:

        # Score Production - MOVING TO SCORE - #############################################################################################
        model_stage = "production"
        model_uri = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage)
        with_predictions = fs.score_batch(model_uri, taxi_data)

        # Get Latest Version: Which is the the model you have just trained
        latest_model_version = get_latest_model_version(model_name)
        model_uri = "models:/{model_name}/{latest_model_version}".format(model_name=model_name, latest_model_version=latest_model_version)
        with_predictions = fs.score_batch(model_uri, taxi_data)


        import pyspark.sql.functions as func
        cols = ['prediction', 'fare_amount', 'trip_distance', 'pickup_zip', 'dropoff_zip', 
                'rounded_pickup_datetime', 'rounded_dropoff_datetime', 'mean_fare_window_1h_pickup_zip', 
                'count_trips_window_1h_pickup_zip', 'count_trips_window_30m_dropoff_zip', 'dropoff_is_weekend']

        with_predictions_reordered = (
            with_predictions.select(
                cols,
            )
            .withColumnRenamed(
                "prediction",
                "predicted_fare_amount",
            )
            .withColumn(
            "predicted_fare_amount",
            func.round("predicted_fare_amount", 2),
            )
        )
        display(with_predictions_reordered)
        # Get the R2 etc. ####################################################################################################################
        




        # CREATE LOGIC DEFINING WHEN TO PROMOTE MODEL (EVALUATION)
        is_improvement = True
        ##########################################################
        
        if is_improvement:

            # MOVE TO "REGISTRATION" SCRIPTS - CALL FUNCTION FROM HERE
            model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

            # wait until the reigstered model is ready
            wait_until_ready(model_details.name, model_details.version, client)

            client.update_registered_model(
            name=model_details.name,
            description="Insert"
            )

            client.update_model_version(
            name=model_details.name,
            version=model_details.version,
            description="Insert"
            )
            
            # Demote Staging to None
            staging_stage = 'staging'
            no_stage = None
            # Get the latest model version in the staging stage
            latest_production_version = mlflow.get_latest_versions(
                name=model_name,
                stages=[staging_stage],
                order_by=['creation_time desc'],
                max_results=1
            )[0].version
            
            mlflow.transition_model_version_stage(
                name=model_name,
                version=latest_production_version,
                stage=no_stage
            )


            # Demote Production To Staging (Keeps Incumbent Model As A BackStop)
            production_stage = 'production'
            staging_stage = 'staging'
            # Get the latest model version in the production stage
            latest_production_version = mlflow.get_latest_versions(
                name=model_name,
                stages=[production_stage],
                order_by=['creation_time desc'],
                max_results=1
            )[0].version

            # Demote the latest model version from production to staging
            mlflow.transition_model_version_stage(
                name=model_name,
                version=latest_production_version,
                stage=staging_stage
            )


            # Get latest registered model. This is the challenger that will be promoted to Production
            latest_registered_version = mlflow.get_latest_versions(
                name=model_name,
                order_by=['creation_time desc'],
                max_results=1
            )[0].version

            mlflow.transition_model_version_stage(
                name=model_name,
                version=latest_registered_version,
                stage=production_stage
            )



    # Get the model URI
    latest_model_version = get_latest_model_version(model_name)
    model_uri = f"models:/taxi_example_fare_packaged/{latest_model_version}"
    with_predictions = fs.score_batch(model_uri, taxi_data)
    #If there is no model registered with this name, then register it, and promote it to production. 

    # If there is a model that is registered and in productionstage , then 1. load it, 2. score it. 
    # 3. Load model that you've just logged. compare the results. 
    # 4. If better then promote most recent version of model to production stage, and demote current production to stage


    # COMMAND ----------
    #latest_pyfunc_version = get_latest_model_version("pyfunc_taxi_fare_packaged")
    #pyfunc_model_uri = f"models:/pyfunc_taxi_fare_packaged/{latest_pyfunc_version}"
    #pyfunc_predictions = fs.score_batch(pyfunc_model_uri, 
    #                                taxi_data,
    #                                result_type='string')


    # COMMAND ----------
    import pyspark.sql.functions as func
    cols = ['prediction', 'fare_amount', 'trip_distance', 'pickup_zip', 'dropoff_zip', 
            'rounded_pickup_datetime', 'rounded_dropoff_datetime', 'mean_fare_window_1h_pickup_zip', 
            'count_trips_window_1h_pickup_zip', 'count_trips_window_30m_dropoff_zip', 'dropoff_is_weekend']
    with_predictions_reordered = (
        with_predictions.select(
            cols,
        )
        .withColumnRenamed(
            "prediction",
            "predicted_fare_amount",
        )
        .withColumn(
        "predicted_fare_amount",
        func.round("predicted_fare_amount", 2),
        )
    )
    display(with_predictions_reordered)

    # COMMAND ----------
    #display(pyfunc_predictions.select('fare_amount', 'prediction'))

    # COMMAND ----------

if __name__ == "__main__":
    fs = feature_store.FeatureStoreClient()
    model_name = "taxi_example_fare_packaged"
    taxi_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")
    run_id = mlflow.active_run().info.run_id
    
    # Do not log 

    # training_set will cbe returned from another function. 
    training_set = []
    client = MlflowClient()
    evaluation(fs=fs, taxi_data=taxi_data, model_name=model_name, training_set=training_set, run_id=run_id, client=client)













