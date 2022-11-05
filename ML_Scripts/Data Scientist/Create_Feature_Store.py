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

# COMMAND ----------

display(df_train.describe())

# COMMAND ----------

df_train.printSchema()

# COMMAND ----------

# Renaming Columns 

df_train = (df_train 
                 .withColumnRenamed("Pclass", "PassengerClass") 
                 .withColumnRenamed("SibSp", "SiblingsSpouses") 
                 .withColumnRenamed("Parch", "ParentsChildren"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The Above is Probable best described as Data Engineering / ETL 
# MAGIC 
# MAGIC Below is more Feature Engineering 

# COMMAND ----------

df_train.select("Name")

# These titles provides information on social status, profession, etc.
# Extract Title from Name, store in column "Title"
df = df_train.withColumn("Title",regexp_extract(col("Name"),"([A-Za-z]+)\.",1))

# COMMAND ----------

# This new column 'Title' is actually a new feature for your data set!
display(df)

# COMMAND ----------

# Sanitise and group titles
# 'Mlle', 'Mme', 'Ms' --> Miss
# 'Lady', 'Dona', 'Countess' --> Mrs
# 'Dr', 'Master', 'Major', 'Capt', 'Sir', 'Don' --> Mr
# 'Jonkheer' , 'Col' , 'Rev' --> Other
df = df.replace(['Mlle','Mme', 'Ms', 'Dr','Master','Major','Lady','Dona','Countess','Jonkheer','Col','Rev','Capt','Sir','Don'],
                ['Miss','Miss','Miss','Mr','Mr', 'Mr', 'Mrs',  'Mrs', 'Mrs', 'Other',  'Other','Other','Mr','Mr','Mr'])

# COMMAND ----------

# Did they have a Cabin?
df = df.withColumn('Has_Cabin', df.Cabin.isNotNull())

# COMMAND ----------

# Create a new feature called "Family_size". This feature is the summation of ParentsChildren (parents/children) and SiblingsSpouses(siblings/spouses). It gives us a combined data so that we can check if survival rate have anything to do with family size of the passengers
df = df.withColumn("Family_Size", col('SiblingsSpouses') + col('ParentsChildren') + 1)

# COMMAND ----------

# Let's organise the newly created features in a dataframe
titanic_feature = df.select("Name","Cabin","Title","Has_Cabin","Family_Size")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS feature_store_titanic;

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

fs.create_table(
    name="feature_store_titanic.titanic_passengers_features_2",
    primary_keys = ["Name","Cabin"],
    df = titanic_feature,
    description = "Titanic Passenger Features")

# COMMAND ----------

# Write the features DataFrame to the feature store table
# 'merge' - upserts rows in df into the feature table.
# 'overwrite' - updates whole table.

fs.write_table(
  name = "feature_store_titanic.titanic_passengers_features_2",
  df = titanic_feature,
  mode = "merge")

# COMMAND ----------

# Get feature table’s metadata

ft = fs.get_table("feature_store_titanic.titanic_passengers_features_2")
print (ft.keys)
print (ft.description)

# COMMAND ----------

df = fs.read_table("feature_store_titanic.titanic_passengers_features_2")
display(df)
