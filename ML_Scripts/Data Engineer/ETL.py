# Databricks notebook source
# Take Data and Transform it, So that it may be used in the Feature Engineering stage

# Raw Data Container --> ETL --> Feature Data Container 

import numpy as np
import pandas as pd 
from pyspark.sql.functions import *
from databricks import feature_store

# COMMAND ----------

# Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
# Author: Dhyanendra Singh Rathore


DBX_SP_Client_Secret = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret")
DBX_SP_ClientID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_ClientID")
DBX_SP_TenantID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_TenantID")


# Ensure container access is set to public access
# Ensure that RABC Blob contributor is set onto the container for the SP????


spark.conf.set("fs.azure.account.auth.type.testsachd.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.testsachd.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.testsachd.dfs.core.windows.net", "7e83398e-8440-4d0c-8533-44c8b90bfa83")
spark.conf.set("fs.azure.account.oauth2.client.secret.testsachd.dfs.core.windows.net", "kCq8Q~P2Do0CET~h8zauDNKMULVZ3uuZu~bsXah8")   
spark.conf.set("fs.azure.account.oauth2.client.endpoint.testsachd.dfs.core.windows.net", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
display(dbutils.fs.ls("abfss://new@testsachd.dfs.core.windows.net/hvkgv"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")


spark.conf.set("fs.azure.account.auth.type.adlsdevgayt.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsdevgayt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsdevgayt.dfs.core.windows.net", "7e83398e-8440-4d0c-8533-44c8b90bfa83")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsdevgayt.dfs.core.windows.net", "kCq8Q~P2Do0CET~h8zauDNKMULVZ3uuZu~bsXah8")   
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsdevgayt.dfs.core.windows.net", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
display(dbutils.fs.ls("abfss://raw@adlsdevgayt.dfs.core.windows.net"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")








# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/csvFiles"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

df = spark.read.format("text").load("/mnt/test/train.csv")

# COMMAND ----------

DBX_SP_Client_Secret = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret")
DBX_SP_ClientID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_ClientID")
DBX_SP_TenantID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_TenantID")

print(DBX_SP_Client_Secret)
print(DBX_SP_ClientID)
print(DBX_SP_TenantID)

spark.conf.set("fs.azure.account.auth.type.adlsdevgayt.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsdevgayt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsdevgayt.dfs.core.windows.net", DBX_SP_ClientID)
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsdevgayt.dfs.core.windows.net", DBX_SP_Client_Secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsdevgayt.dfs.core.windows.net", "https://login.microsoftonline.com/"+DBX_SP_TenantID+"/oauth2/token")





dbfs_path = 'dbfs:/FileStore/tables/train.csv'
df_train = spark.read.csv(dbfs_path, header = "True", inferSchema="True")

# COMMAND ----------


display(df_train)

# COMMAND ----------

# Display training data
2
display(df_train)

# COMMAND ----------

df_train.printSchema()
