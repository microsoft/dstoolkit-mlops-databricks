# Databricks notebook source

from argparse import ArgumentParser
from pathlib import Path
from datetime import datetime
from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone
from ds_utils import *
from pyspark.sql import SparkSession
import logging

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
def dropoff_features_fn(
        df,
        ts_column, 
        start_date, 
        end_date
    ):

    '''
    Returns features for dropoff zip codes

            Parameters:
                    df (df): A dataframe containing the data
                    ts_column (col):
                    start_date (datetime): Start date of the data
                    end_date (datetime): End date of the data

            Returns:
                    dropoffzip_features (df): A dataframe containing the features for dropoff zip codes
    '''

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
    '''
    Returns features for pickup zip codes

            Parameters:
                    df (df): A dataframe containing the data
                    ts_column (col):
                    start_date (datetime): Start date of the data
                    end_date (datetime): End date of the data

            Returns:
                    pickupzip_features (df): A dataframe containing the features for pickup zip codes
    '''

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
# COMMAND ----------

def fs_create(fs,
            pickup_features, 
            dropoff_features,
            feature_table_name_1,
            feature_Table_name_2,
            spark
        ):

    '''
    Create Feature Store Tables: If they do not exist

            Parameters:
                    fs (feature_store): A feature store client
                    pickup_features (df): A dataframe containing the features for pickup zip codes
                    dropoff_features (df): A dataframe containing the features for dropoff zip codes
                    feature_table_name_1 (str): Name of the feature store table for pickup zip codes
                    feature_Table_name_2 (str): Name of the feature store table for dropoff zip codes
                    spark (spark): A spark session

            Returns:
                    None
    '''

    if not spark.catalog.tableExists(feature_table_name_1) & spark.catalog.tableExists(feature_Table_name_2):
        # Create Logic that if table doesn't exist, then to create it with the features created above
        
        fs.create_table(
            name=feature_table_name_1,
            primary_keys=["zip", "ts"],
            df=pickup_features,
            partition_columns="yyyy_mm",
            description="Taxi Fares. Pickup Features",
        )

        fs.create_table(
            name=feature_Table_name_2,
            primary_keys=["zip", "ts"],
            df=dropoff_features,
            partition_columns="yyyy_mm",
            description="Taxi Fares. Dropoff Features",
        )

        logging.warning('Feature Store Tables Created')
# COMMAND ----------

def update_fs(
        pickup_features_df,
        dropoff_features_df,
        feaure_table_name_1, 
        feature_table_name_2, 
        fs
    ):
    '''
    Update Feature Store Tables: If they do exist
    
            Parameters:
                    pickup_features_df (df): A dataframe containing the features for pickup zip codes
                    dropoff_features_df (df): A dataframe containing the features for dropoff zip codes
                    feaure_table_name_1 (str): Name of the feature store table for pickup zip codes
                    feature_table_name_2 (str): Name of the feature store table for dropoff zip codes
                    fs (feature_store): A feature store client

            Returns:
                    None
    '''

    fs.write_table(
        name=feaure_table_name_1,
        df=pickup_features_df,
        mode="merge"
    )

    fs.write_table(
        name=feature_table_name_2,
        df=dropoff_features_df,
        mode="merge"
    )
# COMMAND ----------

def get_data(delta_table_name, spark):
    '''
    Retrieve Data from Delta Lake

            Parameters:
                    delta_table_name (str): Name of the delta table
                    spark (spark): A spark session

            Returns:
                    new_data (df): A dataframe containing the data
    '''

    logging.warning('Retrieving Data from Delta Lake')
    new_data = spark.read.table(delta_table_name)
    
    if new_data:
        logging.warning('Data Retrieved')
        return new_data
    
    logging.warning('Error Retrieving Data')
    assert type(new_data) != None
# COMMAND ----------  

def run_feature_store_refresh():
    '''
    Run Feature Store Refresh
    '''

    # Create Spark Session
    spark = SparkSession.builder.getOrCreate()
    #import pdb; pdb.set_trace()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    
    # Create Feature Store Client
    fs = feature_store.FeatureStoreClient()
    
    # Get Data
    delta_table_name = "feature_store_taxi_example.nyc_yellow_taxi_with_zips"
    new_data = get_data(delta_table_name=delta_table_name, spark=spark)
    #raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")

    #Initial Feature Store Creation Features
    pickup_features = pickup_features_fn(
        new_data, 
        ts_column="tpep_pickup_datetime", 
        start_date=datetime(2016, 1, 1), 
        end_date=datetime(2016, 1, 15)
    )

    dropoff_features = dropoff_features_fn(
        new_data, 
        ts_column="tpep_dropoff_datetime", 
        start_date=datetime(2016, 1, 1), 
        end_date=datetime(2016, 1, 15)
    )


    # If the Tables do not exist
    feaure_table_name_1 = "feature_store_taxi_example.trip_pickup_features"
    feature_table_name_2 = "feature_store_taxi_example.trip_dropoff_features"
    fs_create(
        pickup_features=pickup_features,
        dropoff_features=dropoff_features,
        fs=fs,
        feature_table_name_1=feaure_table_name_1, 
        feature_Table_name_2=feature_table_name_2,
        spark=spark
    )


    # Update Feature Store With New Features (Date Range Changed)
    new_pickup_features = pickup_features_fn(
        new_data, 
        ts_column="tpep_pickup_datetime", 
        start_date=datetime(2016, 1, 15), 
        end_date=datetime(2016, 5, 29)
    )


    new_dropoff_features = dropoff_features_fn(
        new_data, 
        ts_column="tpep_dropoff_datetime", 
        start_date=datetime(2016, 1, 15), 
        end_date=datetime(2016, 5, 29)
    )


    # Feature Store Update
    update_fs(
        pickup_features_df=new_pickup_features,
        dropoff_features_df=new_dropoff_features,
        feaure_table_name_1=feaure_table_name_1,
        feature_table_name_2=feature_table_name_2,
        fs=fs
    )
# COMMAND ----------

if __name__ == "__main__":
    run_feature_store_refresh()
