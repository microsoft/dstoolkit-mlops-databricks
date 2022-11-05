
# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic 👋🛳️ Ahoy!
# MAGIC This is the legendary Titanic ML dataset
# MAGIC The goal is simple: utilise machine learning to develop a model that predicts which passengers survived the Titanic shipwreck. To generate the optimal model, however, the features in the dataset must go through a feature engineering procedure.

# COMMAND ----------

# MAGIC %md
# MAGIC # Background 
# MAGIC * The sinking of the Titanic is one of the most infamous shipwrecks in history.
# MAGIC * On April 15, 1912, during her maiden voyage, the widely considered “unsinkable” RMS Titanic sank after colliding with an iceberg. Unfortunately, there weren’t enough lifeboats for everyone onboard, resulting in the death of 1502 out of 2224 passengers and crew.
# MAGIC * While there was some element of luck involved in surviving, it seems some groups of people were more likely to survive than others.
# MAGIC * This is about building a predictive model that is able to predict which passengers survived using passenger data (ie name, age, gender, socio-economic class, etc).

# COMMAND ----------

# MAGIC %md  
# MAGIC #  1. Setup

# COMMAND ----------

import numpy as np
import pandas as pd 
from pyspark.sql.functions import *
from databricks import feature_store

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Import Dataset ⬇
# MAGIC Training Dataset: `train` 🚢

# COMMAND ----------

#Alter this path to point to the location of where you have uploaded your train.csv
dbfs_path = 'dbfs:/FileStore/tables/train.csv'
df_train = spark.read.csv(dbfs_path, header = "True", inferSchema="True")

# COMMAND ----------

# Display training data
display(df_train)

# COMMAND ----------

# MAGIC %md ### Summary of Data

# COMMAND ----------

display(df_train.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking Schema of our dataset

# COMMAND ----------

df_train.printSchema()

# COMMAND ----------

# MAGIC %md # 3. Cleaning Data 🧹

# COMMAND ----------

# MAGIC %md ### Renaming Columns

# COMMAND ----------

df_train = (df_train 
                 .withColumnRenamed("Pclass", "PassengerClass") 
                 .withColumnRenamed("SibSp", "SiblingsSpouses") 
                 .withColumnRenamed("Parch", "ParentsChildren"))

# COMMAND ----------

# MAGIC %md # 4. Feature Engineering 🛠
# MAGIC Apply feature engineering steps 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passenger's Title

# COMMAND ----------

df_train.select("Name")

# COMMAND ----------

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

# MAGIC %md
# MAGIC ###  Passenger's Cabins

# COMMAND ----------

# Did they have a Cabin?
df = df.withColumn('Has_Cabin', df.Cabin.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Family Sizes of Passengers

# COMMAND ----------

# Create a new feature called "Family_size". This feature is the summation of ParentsChildren (parents/children) and SiblingsSpouses(siblings/spouses). It gives us a combined data so that we can check if survival rate have anything to do with family size of the passengers
df = df.withColumn("Family_Size", col('SiblingsSpouses') + col('ParentsChildren') + 1)

# COMMAND ----------

# Let's organise the newly created features in a dataframe
titanic_feature = df.select("Name","Cabin","Title","Has_Cabin","Family_Size")

# COMMAND ----------

# MAGIC %md 
# MAGIC # 5. Use Feature Store library to create new feature tables 💻

# COMMAND ----------

# MAGIC %md First, create the database where the feature tables will be stored.

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS feature_store_titanic;

# COMMAND ----------

# MAGIC %md ## Instantiate a Feature Store client and create table

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

# MAGIC %md
# MAGIC ##Get feature table’s metadata

# COMMAND ----------

ft = fs.get_table("feature_store_titanic.titanic_passengers_features_2")
print (ft.keys)
print (ft.description)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reads contents of feature table

# COMMAND ----------

df = fs.read_table("feature_store_titanic.titanic_passengers_features_2")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC You can now discover the feature tables in the <a href="#feature-store/" target="_blank">Feature Store UI</a>.

# COMMAND ----------

# MAGIC %md 
# MAGIC # 6. ML model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create training dataset

# COMMAND ----------

from databricks.feature_store import FeatureLookup

titanic_features_table = "feature_store_titanic.titanic_passengers_features_2"

# We choose to only use 2 of the newly created features
titanic_features_lookups = [
    FeatureLookup( 
      table_name = titanic_features_table,
      feature_names = "Title",
      lookup_key = ["Name"],
    ),
    FeatureLookup( 
      table_name = titanic_features_table,
      feature_names = "Has_Cabin",
      lookup_key = ["Name","Cabin"],
    ),
#     FeatureLookup( 
#       table_name = titanic_features_table,
#       feature_names = "Family_Size",
#       lookup_key = ["Name"],
#     ),
]

# Create the training set that includes the raw input data merged with corresponding features from both feature tables
exclude_columns = ["Name", "PassengerId","ParentsChildren","SiblingsSpouses","Ticket"]
training_set = fs.create_training_set(
                df_train,
                feature_lookups = titanic_features_lookups,
                label = 'Survived',
                exclude_columns = exclude_columns
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------

# MAGIC %md
# MAGIC Train a LightGBM model on the data, then log the model with MLFlow. The model will be packaged with feature metadata.

# COMMAND ----------

from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient
import lightgbm as lgb
import mlflow
from sklearn.metrics import accuracy_score

# Load the TrainingSet into a dataframe which can be passed into sklearn for training a model
training_df = training_set.load_df()

# End any existing runs (in the case this notebook is being run for a second time)
mlflow.end_run()

# Start an mlflow run, which is needed for the feature store to log the model
mlflow.start_run(run_name="lgbm_feature_store") 

data = training_df.toPandas()
data_dum = pd.get_dummies(data, drop_first=True)

# Extract features & labels
X = data_dum.drop(["Survived"], axis=1)
y = data_dum.Survived

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)

lgb_params = {
            'n_estimators': 50,
            'learning_rate': 1e-3,
            'subsample': 0.27670395476135673,
            'colsample_bytree': 0.6,
            'reg_lambda': 1e-1,
            'num_leaves': 50, 
            'max_depth': 8, 
            }

mlflow.log_param("hyper-parameters", lgb_params)
lgbm_clf  = lgb.LGBMClassifier(**lgb_params)
lgbm_clf.fit(X_train,y_train)
lgb_pred = lgbm_clf.predict(X_test)

accuracy=accuracy_score(lgb_pred, y_test)
print('LightGBM Model accuracy score: {0:0.4f}'.format(accuracy_score(y_test, lgb_pred)))
mlflow.log_metric('accuracy', accuracy)

# COMMAND ----------

# Log the trained model with MLflow and package it with feature lookup information. 
fs.log_model(
  lgbm_clf,
  artifact_path = "model_packaged",
  flavor = mlflow.sklearn,
  training_set = training_set,
  registered_model_name = "titanic_packaged"
)

# COMMAND ----------


