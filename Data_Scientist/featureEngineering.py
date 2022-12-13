# Databricks notebook source


# COMMAND ----------

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

# Ingest Parameters Files


p = ArgumentParser()
p.add_argument("--env", required=False, type=str)
namespace = p.parse_known_args(sys.argv[1:])[0]
display(namespace)


if namespace.env is not None:
    display(namespace.env)
    params = yaml.safe_load(pathlib.Path(namespace.env).read_text())
    display(params)
    experiment_name = params['ML_PIPELINE_FILES']['FEATURE_ENGINEERING']['PARAMETERS']['EXPERIMENT_NAME']
    display(experiment_name)
    mlflow.set_experiment(experiment_name=experiment_name) 

else:
    display("Set The Parameters Manually, As We Are Deploying From UI")
    mlflow.set_experiment("/Shared/dbxDevelopment") 



# COMMAND ----------

#Inggest Data

raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(raw_data)


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


pickup_features = pickup_features_fn(
    raw_data, ts_column="tpep_pickup_datetime", start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)
dropoff_features = dropoff_features_fn(
    raw_data, ts_column="tpep_dropoff_datetime", start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)

display(pickup_features)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS feature_store_taxi_example;")

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

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
