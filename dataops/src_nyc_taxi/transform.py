# Databricks notebook source


# COMMAND ----------

# MAGIC %md ## Define data preprocessing helper functions

# COMMAND ----------

from uszipcode import SearchEngine
import sqlite3
import pandas as pd
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
import math
from urllib import request
import os

BAD_ZIPCODE_VALUE = 'bad_zipcode'
file_location = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/"
file_type = "csv"
target_year = 2016

def push_zipcode_data_to_executors():
  # Download directly from github since the default download location can be flaky
  target_dir = '/tmp/db/'
  print(target_dir)
  target_file = os.path.join(target_dir, 'simple_db.sqlite')
  print(target_file)
  remote_url = 'https://github.com/MacHu-GWU/uszipcode-project/files/5183256/simple_db.log'
  os.makedirs(target_dir, exist_ok=True)
  print(os.makedirs(target_dir, exist_ok=True))
  request.urlretrieve(remote_url, target_file)
  print(request.urlretrieve(remote_url, target_file))
  # Query the zipcode database into a pandas dataframe
  #search = SearchEngine(db_file_dir=target_dir)
  conn = sqlite3.connect(target_file)
  pdf = pd.read_sql_query('''select  zipcode, lat, lng, radius_in_miles, 
                          bounds_west, bounds_east, bounds_north, bounds_south from 
                          simple_zipcode''',conn)
  return sc.broadcast(pdf)
  
# Define UDF to lookup ZIP code based on latitude and longitude
@udf('string')
def get_zipcode(lat, lng):
    if lat is None or lng is None:
      return BAD_ZIPCODE_VALUE
    dist_btwn_lat_deg = 69.172
    dist_btwn_lon_deg = math.cos(lat) * 69.172
    radius = 5
    lat_degr_rad = abs(radius / dist_btwn_lat_deg)
    lon_degr_rad = abs(radius / dist_btwn_lon_deg)
    lat_lower = lat - lat_degr_rad
    lat_upper = lat + lat_degr_rad
    lng_lower = lng - lon_degr_rad
    lng_upper = lng + lon_degr_rad
    pdf = zipcodes_broadcast_df.value
    try:
        out = pdf[(pdf['lat'].between(lat_lower, lat_upper)) & (pdf['lng'].between(lng_lower, lng_upper))]
        dist = [None]*len(out)
        for i in range(len(out)):
            dist[i] = (out['lat'].iloc[i]-lat)**2 + (out['lng'].iloc[i]-lng)**2
        zip = out['zipcode'].iloc[dist.index(min(dist))]
    except:
        zip = BAD_ZIPCODE_VALUE
    return zip
  
def get_data_files(yyyy, months):
  data_files = []
  for mm in months:
    mm = str(mm) if mm >= 10 else f"0{mm}"
    month_data_files = list(filter(lambda file_name: f"{yyyy}-{mm}" in file_name,
                           [f.path for f in dbutils.fs.ls(file_location)]))
    data_files += month_data_files
  return data_files
  
def load_data(data_files, sample=1.0):
  df = (spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("sep", ",")
        .load(data_files)
      ).sample(False, sample, 123)
  
  # Rename, cast types, and filter columns
  column_allow_list = { 
    "pickup_datetime": ["tpep_pickup_datetime", "timestamp"],
    "tpep_pickup_datetime": ["tpep_pickup_datetime", "timestamp"],
    
    # type conversion
    "dropoff_datetime": ["tpep_dropoff_datetime", "timestamp"],
    "tpep_dropoff_datetime": ["tpep_dropoff_datetime", "timestamp"],
    
    "pickup_zip": ["pickup_zip", "integer"],
    "dropoff_zip": ["dropoff_zip", "integer"],
    "trip_distance": ["trip_distance", "double"],
    "fare_amount": ["fare_amount", "double"],
    "pickup_latitude": ["pickup_latitude", "double"],
    "pickup_longitude": ["pickup_longitude", "double"],
    "dropoff_latitude": ["dropoff_latitude", "double"],
    "dropoff_longitude": ["dropoff_longitude", "double"],
  }
  columns = []
  for orig in df.columns:
    orig_lower = orig.lower()
    if orig_lower in column_allow_list:
      new_name, data_type = column_allow_list[orig_lower]
      columns.append(col(orig).cast(data_type).alias(new_name.lower()))
  
  return df.select(columns)  

def annotate_zipcodes(df):
  to_zip = lambda lat, lng:  get_zipcode(col(lat).astype("double"), col(lng).astype("double"))
  # Add ZIP code columns, drop intermediate columns
  df = (df
          .withColumn('pickup_zip', to_zip("pickup_latitude", "pickup_longitude"))
          .withColumn('dropoff_zip', to_zip("dropoff_latitude", "dropoff_longitude"))
          .drop('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude')
         )
  # Filter out rows with bad data
  df = df.filter(df.pickup_zip != BAD_ZIPCODE_VALUE)
  df = df.filter(df.dropoff_zip != BAD_ZIPCODE_VALUE)
  
  # Cast ZIP code to int
  df = df.withColumn("pickup_zip", df["pickup_zip"].cast(IntegerType()))
  df = df.withColumn("dropoff_zip", df["dropoff_zip"].cast(IntegerType()))
  return df

def write_to_table(df, database, table):
  (df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{database}.{table}"))


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS feature_store_taxi_example;")

# COMMAND ----------

# MAGIC %md ## Generate DataFrame and write to table

# COMMAND ----------

# Read ZIP code data and push a broadcast dataframe to executors to speed up the UDF
zipcodes_broadcast_df = push_zipcode_data_to_executors()

# Generate data file names for the first 2 months of data in 2016
data_files = get_data_files(target_year,months=[1,2])

# Load in a small subsample of data to speed things up for this example
df = load_data(data_files, sample=.001)

# Repartition -- by default this dataset only has a single partition.  
# Use a small parition count since the dataset is already small.
df = df.repartition(6)

# Enhance the DataFrame by converting latitude and longitude coordinates into ZIP codes 
df_with_zip = annotate_zipcodes(df)

# Write the DataFrame to a Delta table
write_to_table(df_with_zip, database="feature_store_taxi_example", table="nyc_yellow_taxi_with_zips")

# COMMAND ----------

raw_data = spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")

# COMMAND ----------

display(raw_data)
