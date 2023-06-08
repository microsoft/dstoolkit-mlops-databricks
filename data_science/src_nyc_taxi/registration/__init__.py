# Databricks notebook source

from databricks.sdk.runtime import *
from databricks import feature_store
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
import mlflow
from mlflow.tracking import MlflowClient
from databricks import feature_store
from ds_utils import *

# COMMAND ----------

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


def get_model_uri(
    fs,
    model_name,
    model_stage
    ):

    fs = feature_store.FeatureStoreClient()

    model_uri_production = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage)

    return model_uri_production

def get_data(feature_table_name):
    
    
    taxi_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")
    taxi_data = rounded_taxi_data(spark, taxi_data_df = taxi_data)

    return taxi_data

def predict(
    fs,
    model_uri,
    taxi_data
    ):

    with_predictions = fs.score_batch(
        model_uri,
        taxi_data
    )

    expected_y = with_predictions.select('fare_amount').toPandas()
    predicted_y = with_predictions.select('prediction').toPandas()

    from sklearn import metrics
    r2 = metrics.r2_score(
        expected_y, 
        predicted_y
        )

    display(expected_y)
    display(with_predictions)

    print(f"R2: {r2}")

    # Display Data For Demo Purposes

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

    return r2

def evaluation(
    score_latest_model,
    score_production_model
    ):
    model_name = "taxi_example_fare_packaged"

    if score_latest_model > score_production_model:
        print("Latest Model Is Better Than Production Model")
        
        # Demote Production
        production_stage = 'production'

        # Get the latest model version in the production stage

        mlflow_client = MlflowClient()

        latest_production_version = mlflow_client.get_latest_versions(
            name=model_name,
            stages=[production_stage])[0].version

        #print(latest_production_version[0].version)
        #print(type(latest_production_version))


        # Promote Latest Model To Production
        latest_model_version = get_latest_model_version(model_name)
        mlflow_client.transition_model_version_stage(
                name=model_name,
                version=latest_model_version,
                stage="production",
                archive_existing_versions = True
        )


def run_registration(model_name):
    fs = feature_store.FeatureStoreClient()

    latest_model_version = get_latest_model_version(model_name)

    taxi_data = get_data(
        feature_table_name = "feature_store_taxi_example.nyc_yellow_taxi_with_zips"
    )

    model_uri_latest = get_model_uri(
        fs=fs,
        model_name = model_name,
        model_stage = "latest"

    )

    print(model_uri_latest)

    model_uri_production = get_model_uri(
        fs=fs,
        model_name = model_name,
        model_stage = "production"
    )

    print(model_uri_production)

    new = production_model_exists(
        model_name = model_name,
        model_stage = "production"
    )

    print(new)

    if production_model_exists(
        model_name = model_name,
        model_stage = "Production"
        ):

        score_latest_model = predict(
            fs = fs,
            model_uri = model_uri_latest,
            taxi_data = taxi_data
        )

        score_production_model = predict(
            fs = fs,
            model_uri = model_uri_production,
            taxi_data = taxi_data
        )

        evaluation(
            score_latest_model = score_latest_model,
            score_production_model = score_production_model
        )
    else:
        print("No production model found. Promoting latest model to production")
        mlflow_client = MlflowClient()
        mlflow_client.transition_model_version_stage(
            name="taxi_example_fare_packaged",
            version=latest_model_version,
            stage="production",
            archive_existing_versions = True
        )


def production_model_exists(
    model_name,
    model_stage
    ):


    mlflow_client = MlflowClient()
    for mv in mlflow_client.search_model_versions("name = '%s'" % model_name):
        if mv.current_stage == model_stage:
            return True

# COMMAND ----------

if __name__ == "__main__":
    run_registration(
        model_name = "taxi_example_fare_packaged"
    )

    #latest_model_version = get_latest_model_version(model_name)
    #mlflow_client = MlflowClient()
    #mlflow_client.get_latest_versions(name=model_name, stages=[model_stage], order_by=['creation_time desc'], max_results=1)
    #experiment = mlflow_client.get_experiment_by_name("/Shared/ciaran_experiment_nyc_taxi")
    #experiment_id = experiment.experiment_id

    # INCREDIBLY IMPORTANT - "runs" IS GIVING US EVERYTHING, INCLUDING R2 AND PARAMETERS - USE THIS FOR POWERBI
    #runs = mlflow.search_runs(
    #    experiment_ids=experiment_id
    #)
    #display(runs)
    #runs_2 = mlflow.search_runs(
    #    experiment_ids=experiment_id,
    #    filter_string=f"tags.model_version = '{latest_model_version}'")
    #display(runs_2)
    #model_uri = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage)
    #model_uri = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage="latest")