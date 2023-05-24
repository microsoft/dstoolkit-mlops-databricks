# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_flow_v3.png"/>

# COMMAND ----------

# Install python helper function wheel file (in dist) to cluster 
# Install pypi packages azureml-sdk[databricks], lightgbm, uszipcode
# The above will be automated in due course 

# Packages Install.

import mlflow 
import sys
import yaml
import pathlib
from argparse import ArgumentParser
from pathlib import Path
from datetime import datetime
from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone
from helperFunctions.helperFunction import *

# COMMAND ----------

#Inggest Data
raw_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")
#raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(raw_data)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC From the taxi fares transactional data, we will compute two groups of features based on trip pickup and drop off zip codes.
# MAGIC
# MAGIC #### Pickup features
# MAGIC 1. Count of trips (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 1. Mean fare amount (time window = 1 hour, sliding window = 15 minutes)
# MAGIC
# MAGIC #### Drop off features
# MAGIC 1. Count of trips (time window = 30 minutes)
# MAGIC 1. Does trip end on the weekend (custom feature using python code)
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_computation_v5.png"/>

# COMMAND ----------

@udf(returnType=IntegerType())
def is_weekend(dt):
    tz = "America/New_York"
    return int(dt.astimezone(timezone(tz)).weekday() >= 5)  # 5 = Saturday, 6 = Sunday

@udf(returnType=StringType())  
def partition_id(dt):
    # datetime -> "YYYY-MM"
    return f"{dt.year:04d}-{dt.month:02d}"


def filter_df_by_ts(df, ts_column, start_date, end_date):
    if ts_column and start_date:
        df = df.filter(col(ts_column) >= start_date)
    if ts_column and end_date:
        df = df.filter(col(ts_column) < end_date)
    return df


# COMMAND ----------

# MAGIC %md ### Data scientist's custom code to compute features

# COMMAND ----------


pickup_features = pickup_features_fn(
    raw_data, ts_column="tpep_pickup_datetime", start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)
dropoff_features = dropoff_features_fn(
    raw_data, ts_column="tpep_dropoff_datetime", start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)

display(pickup_features)

# COMMAND ----------

# MAGIC %md ### Use Feature Store library to create new time series feature tables

# COMMAND ----------

# MAGIC %md First, create the database where the feature tables will be stored.

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS feature_store_taxi_example;")

# COMMAND ----------

# MAGIC %md Next, create an instance of the Feature Store client.

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md
# MAGIC To create a time series feature table, the DataFrame or schema must contain a column that you designate as the timestamp key. The timestamp key column must be of `TimestampType` or `DateType` and cannot also be a primary key.
# MAGIC
# MAGIC Use the `create_table` API to define schema, unique ID keys, and timestamp keys. If the optional argument `df` is passed, the API also writes the data to Feature Store.

# COMMAND ----------



spark.conf.set("spark.sql.shuffle.partitions", "5")

fs.create_table(
    name="feature_store_taxi_example.trip_pickup_features",
    primary_keys=["zip", "ts"],
    df=pickup_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Pickup Features",
)
fs.create_table(
    name="feature_store_taxi_example.trip_dropoff_features",
    primary_keys=["zip", "ts"],
    df=dropoff_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Dropoff Features",
)

display(raw_data)



# COMMAND ----------

# MAGIC %md ## Update features
# MAGIC
# MAGIC Use the `write_table` function to update the feature table values.
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_compute_and_write.png"/>

# COMMAND ----------


# Compute the pickup_features feature group.


pickup_features_df = pickup_features_fn(
  df=raw_data,
  ts_column="tpep_pickup_datetime",
  start_date=datetime(2016, 2, 1),
  end_date=datetime(2016, 2, 29),
)

# Write the pickup features DataFrame to the feature store table
fs.write_table(
  name="feature_store_taxi_example.trip_pickup_features",
  df=pickup_features_df,
  mode="merge",
)

# Compute the dropoff_features feature group.
dropoff_features_df = dropoff_features_fn(
  df=raw_data,
  ts_column="tpep_dropoff_datetime",
  start_date=datetime(2016, 2, 1),
  end_date=datetime(2016, 2, 29),
)

# Write the dropoff features DataFrame to the feature store table
fs.write_table(
  name="feature_store_taxi_example.trip_dropoff_features",
  df=dropoff_features_df,
  mode="merge",
)

# COMMAND ----------

# MAGIC %md When writing, both `merge` and `overwrite` modes are supported.
# MAGIC
# MAGIC     fs.write_table(
# MAGIC       name="feature_store_taxi_example.trip_pickup_time_series_features",
# MAGIC       df=new_pickup_features,
# MAGIC       mode="overwrite",
# MAGIC     )
# MAGIC     
# MAGIC Data can also be streamed into Feature Store by passing a dataframe where `df.isStreaming` is set to `True`:
# MAGIC
# MAGIC     fs.write_table(
# MAGIC       name="feature_store_taxi_example.trip_pickup_time_series_features",
# MAGIC       df=streaming_pickup_features,
# MAGIC       mode="merge",
# MAGIC     )
# MAGIC     
# MAGIC You can schedule a notebook to periodically update features using Databricks Jobs ([AWS](https://docs.databricks.com/jobs.html)|[Azure](https://docs.microsoft.com/azure/databricks/jobs)|[GCP](https://docs.gcp.databricks.com/jobs.html)).

# COMMAND ----------

# MAGIC %md Analysts can interact with Feature Store using SQL, for example:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(count_trips_window_30m_dropoff_zip) AS num_rides,
# MAGIC   dropoff_is_weekend
# MAGIC FROM
# MAGIC   feature_store_taxi_example.trip_dropoff_features
# MAGIC WHERE
# MAGIC   dropoff_is_weekend IS NOT NULL
# MAGIC GROUP BY
# MAGIC   dropoff_is_weekend;

# COMMAND ----------


