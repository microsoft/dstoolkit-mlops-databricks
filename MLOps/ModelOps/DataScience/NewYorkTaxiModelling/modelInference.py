# Databricks notebook source

# COMMAND ----------
from databricks import feature_store
from pyspark.sql.types import *
from helperFunctions.helperFunction import *
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType


fs = feature_store.FeatureStoreClient()

new_taxi_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
new_taxi_data = rounded_taxi_data(new_taxi_data)


# COMMAND ----------
cols = ['fare_amount', 'trip_distance', 'pickup_zip', 'dropoff_zip', 'rounded_pickup_datetime', 'rounded_dropoff_datetime']
new_taxi_data_reordered = new_taxi_data.select(cols)
display(new_taxi_data_reordered)


# COMMAND ----------
# Get the model URI
latest_model_version = get_latest_model_version("taxi_example_fare_packaged")
model_uri = f"models:/taxi_example_fare_packaged/{latest_model_version}"
with_predictions = fs.score_batch(model_uri, new_taxi_data)


# COMMAND ----------
latest_pyfunc_version = get_latest_model_version("pyfunc_taxi_fare_packaged")
pyfunc_model_uri = f"models:/pyfunc_taxi_fare_packaged/{latest_pyfunc_version}"
pyfunc_predictions = fs.score_batch(pyfunc_model_uri, 
                                  new_taxi_data,
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












