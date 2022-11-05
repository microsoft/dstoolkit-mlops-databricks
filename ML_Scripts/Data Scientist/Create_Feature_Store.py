# Databricks notebook source
# https://www.advancinganalytics.co.uk/blog/2022/2/17/databricks-feature-store-tutorial

import numpy as np
import pandas as pd 
from pyspark.sql.functions import *
from databricks import feature_store

# COMMAND ----------

#Alter this path to point to the location of where you have uploaded your train.csv
dbfs_path = 'dbfs:/FileStore/tables/train.csv'
df_train = spark.read.csv(dbfs_path, header = "True", inferSchema="True")

# COMMAND ----------

# Display training data
display(df_train)
