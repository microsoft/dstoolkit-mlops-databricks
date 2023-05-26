# Databricks notebook source

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
from utils import *
from pyspark.sql import SparkSession



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

def dropoff_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the dropoff_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df,  ts_column, start_date, end_date
    )
    dropoffzip_features = (
        df.groupBy("dropoff_zip", window("tpep_dropoff_datetime", "30 minute"))
        .agg(count("*").alias("count_trips_window_30m_dropoff_zip"))
        .select(
            col("dropoff_zip").alias("zip"),
            unix_timestamp(col("window.end")).alias("ts").cast(IntegerType()),
            partition_id(to_timestamp(col("window.end"))).alias("yyyy_mm"),
            col("count_trips_window_30m_dropoff_zip").cast(IntegerType()),
            is_weekend(col("window.end")).alias("dropoff_is_weekend"),
        )
    )
    return dropoffzip_features 

def pickup_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the pickup_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df, ts_column, start_date, end_date
    )
    pickupzip_features = (
        df.groupBy(
            "pickup_zip", window("tpep_pickup_datetime", "1 hour", "15 minutes")
        )  # 1 hour window, sliding every 15 minutes
        .agg(
            mean("fare_amount").alias("mean_fare_window_1h_pickup_zip"),
            count("*").alias("count_trips_window_1h_pickup_zip"),
        )
        .select(
            col("pickup_zip").alias("zip"),
            unix_timestamp(col("window.end")).alias("ts").cast(IntegerType()),
            partition_id(to_timestamp(col("window.end"))).alias("yyyy_mm"),
            col("mean_fare_window_1h_pickup_zip").cast(FloatType()),
            col("count_trips_window_1h_pickup_zip").cast(IntegerType()),
        )
    )
    return pickupzip_features

def create_fs_tables(fs, raw_data):
    """
        If the feature table does not exist, then create
    
    """
    
    pickup_features = pickup_features_fn(
        raw_data, 
        ts_column="tpep_pickup_datetime", 
        start_date=datetime(2016, 1, 1), 
        end_date=datetime(2016, 1, 15)
    )

    dropoff_features = dropoff_features_fn(
        raw_data, 
        ts_column="tpep_dropoff_datetime", 
        start_date=datetime(2016, 1, 1), 
        end_date=datetime(2016, 1, 15)
    )


    # Create Logic that if table doesn't exist, then to create it with the features created above
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
        # create table 



def fs_refresh(raw_data, spark, start_date, end_date):
    #Read Data


    spark.conf.set("spark.sql.shuffle.partitions", "5")
    fs = feature_store.FeatureStoreClient()

    exists = spark.catalog.tableExists("feature_store_taxi_example.trip_pickup_features") & spark.catalog.tableExists("feature_store_taxi_example.trip_dropoff_features")
    print(exists)
    # If the Tables do not exist
    if not spark.catalog.tableExists("feature_store_taxi_example.trip_pickup_features") & spark.catalog.tableExists("feature_store_taxi_example.trip_dropoff_features"):
       create_fs_tables(fs=fs, raw_data=raw_data) 



    # We now expand the features: note the start and end dates are different (Simulating New Data)
    pickup_features_df = pickup_features_fn(
        df=raw_data,
        ts_column="tpep_pickup_datetime",
        start_date=start_date,
        end_date=end_date,
    )
    dropoff_features_df = dropoff_features_fn(
        df=raw_data,
        ts_column="tpep_dropoff_datetime",
        start_date=start_date,
        end_date=end_date,
    )

    print(dropoff_features_df)

    # Write the expaned pickup features DataFrame to the feature store table
    fs.write_table(
        name="feature_store_taxi_example.trip_pickup_features",
        df=pickup_features_df,
        mode="merge"
    )
    fs.write_table(
        name="feature_store_taxi_example.trip_dropoff_features",
        df=dropoff_features_df,
        mode="merge"
    )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    new_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")
    print(new_data)
    #raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")

    fs_refresh(raw_data=new_data, spark=spark, start_date=datetime(2016, 1, 15), end_date=datetime(2016, 5, 29))
