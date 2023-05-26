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

def evaluation(fs, taxi_data, model_name):
    taxi_data = rounded_taxi_data(taxi_data)

    cols = ['fare_amount', 'trip_distance', 'pickup_zip', 'dropoff_zip', 'rounded_pickup_datetime', 'rounded_dropoff_datetime']
    taxi_data_reordered = taxi_data.select(cols)
    display(taxi_data_reordered)


    # Get the model URI
    latest_model_version = get_latest_model_version(model_name)
    model_uri = f"models:/taxi_example_fare_packaged/{latest_model_version}"

    #If there is no model registered with this name, then register it, and promote it to production. 

    # If there is a model that is registered and in productionstage , then 1. load it, 2. score it. 
    # 3. Load model that you've just logged. compare the results. 
    # 4. If better then promote most recent version of model to production stage, and demote current production to stage


    

    
    with_predictions = fs.score_batch(model_uri, taxi_data)

    print()


    # COMMAND ----------
    latest_pyfunc_version = get_latest_model_version("pyfunc_taxi_fare_packaged")
    pyfunc_model_uri = f"models:/pyfunc_taxi_fare_packaged/{latest_pyfunc_version}"
    pyfunc_predictions = fs.score_batch(pyfunc_model_uri, 
                                    taxi_data,
                                    result_type='string')


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
    display(pyfunc_predictions.select('fare_amount', 'prediction'))

    # COMMAND ----------

if __name__ == "__main__":
    fs = feature_store.FeatureStoreClient()
    model_name = "taxi_example_fare_packaged"
    taxi_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")
    eval(fs=fs, taxi_data=taxi_data, model_name=model_name)













