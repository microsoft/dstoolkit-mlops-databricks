# Databricks notebook source
# Take Data and Transform it, So that it may be used in the Feature Engineering stage
# Raw Data Container --> ETL --> Feature Data Container 


# DATABRICKS SP NEEDS TO HAVE BLOB STORAGE CONTRIBUTOR ACCESS TO THE CONTAINER (OR POSSIBLY JUST THE SA)

import numpy as np
import pandas as pd 
from pyspark.sql.functions import *
#from databricks import feature_store
from pyspark.dbutils import DBUtils


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(spark.version)
print('spark session created.')


#dbutils = DBUtils(spark)
#print(dbutils.fs.ls('dbfs:/FileStore/'))
#print(dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret"))
#dbutils.fs.mounts()


#import subprocess
#import sys
##subprocess.check_call([sys.executable, "-m", "pip", "install", 'azure-storage-blob'])
#subprocess.check_call([sys.executable, "-m", "pip", "install", 'azure-keyvault'])
#subprocess.check_call([sys.executable, "-m", "pip", "install", 'azure-keyvault-secrets'])


# IMPORTANT : https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#authenticate-via-visual-studio-code
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContainerClient
#from azure.keyvault.secrets import SecretClient

account_url = "https://adlsdevgayt.blob.core.windows.net"
default_credential = DefaultAzureCredential()

#client = SecretClient(vault_url="https://keyvault-dev-gayt.vault.azure.net", credential=default_credential)
#print(client)

# Create the BlobServiceClient object
#blob_service_client = BlobServiceClient(account_url, credential=default_credential)
#print(blob_service_client)
#new_container = blob_service_client.create_container("containerfromblobservice")





# COMMAND ----------

# MAGIC %md
# MAGIC # Authenticate To Service Principal - Automated DBFS Mount to ADLS

# COMMAND ----------

# Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
# Author: Dhyanendra Singh Rathore

STORAGE_ACC_NAME = "adlsdevgayt"
DBX_SP_Client_Secret = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret")
DBX_SP_ClientID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_ClientID")
DBX_SP_TenantID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_TenantID")

print(f"Test: {DBX_SP_ClientID}")
print(f"Test: {DBX_SP_Client_Secret}")
print(DBX_SP_TenantID)


# Ensure container access is set to public access
# Ensure that RABC Blob contributor is set onto the container for the SP????


#spark.conf.set("fs.azure.account.auth.type.testsachd.dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type.testsachd.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set("fs.azure.account.oauth2.client.id.testsachd.dfs.core.windows.net", "<>")
#spark.conf.set("fs.azure.account.oauth2.client.secret.testsachd.dfs.core.windows.net", "<>")   
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.testsachd.dfs.core.windows.net", "https://login.microsoftonline.com/<>/oauth2/token")
#spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
#display(dbutils.fs.ls("abfss://new@testsachd.dfs.core.windows.net/hvkgv"))
#spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")


spark.conf.set("fs.azure.account.auth.type."+ STORAGE_ACC_NAME +".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+ STORAGE_ACC_NAME +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+ STORAGE_ACC_NAME +".dfs.core.windows.net", DBX_SP_ClientID )
spark.conf.set("fs.azure.account.oauth2.client.secret."+ STORAGE_ACC_NAME +".dfs.core.windows.net", DBX_SP_Client_Secret )   
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ STORAGE_ACC_NAME +".dfs.core.windows.net", "https://login.microsoftonline.com/"+ DBX_SP_TenantID + "/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
display(dbutils.fs.ls("abfss://raw@"+ STORAGE_ACC_NAME +".dfs.core.windows.net"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

#spark.conf.set("fs.azure.account.auth.type.adlsdevgayt.dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type.adlsdevgayt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set("fs.azure.account.oauth2.client.id.adlsdevgayt.dfs.core.windows.net", "<>")
#spark.conf.set("fs.azure.account.oauth2.client.secret.adlsdevgayt.dfs.core.windows.net", "<>")   
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsdevgayt.dfs.core.windows.net", "https://login.microsoftonline.com/<>/oauth2/token")
#spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
#display(dbutils.fs.ls("abfss://raw@adlsdevgayt.dfs.core.windows.net"))
#spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": DBX_SP_ClientID,
           "fs.azure.account.oauth2.client.secret": DBX_SP_Client_Secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

#display(dbutils.fs.ls("abfss://raw@adlsdevgayt.dfs.core.windows.net"))


# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest CSV From Data Lake & Display

# COMMAND ----------

dbfs_path = 'abfss://raw@adlsdevgayt.dfs.core.windows.net/train.csv'
df_train = spark.read.csv(dbfs_path, header = "True", inferSchema="True")

df_train.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Establish Mount Point

# COMMAND ----------


mount_point = "/mnt/titanicdata"

if all(mount.mountPoint != mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source = "abfss://raw@adlsdevgayt.dfs.core.windows.net/training_data", mount_point = mount_point, extra_configs = configs)
        


# COMMAND ----------

#dbutils.fs.unmount("/mnt/titanicdata")
display(dbutils.fs.mounts())



# COMMAND ----------

# MAGIC %md
# MAGIC # Read in CSV as Data Frame From Mount Point

# COMMAND ----------

df_train = spark.read.csv(mount_point, header = "True", inferSchema="True")
df_train.display()

# COMMAND ----------

df_train.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Cleaning Data

# COMMAND ----------

df_train = (df_train 
                 .withColumnRenamed("Pclass", "PassengerClass") 
                 .withColumnRenamed("SibSp", "SiblingsSpouses") 
                 .withColumnRenamed("Parch", "ParentsChildren"))
df_train.display()

# COMMAND ----------

df_train.display()

# Add a new Feature column "Title"
df = df_train.withColumn("Title",regexp_extract(col("Name"),"([A-Za-z]+)\.",1))
df.display()

# Sanitise and group titles
# 'Mlle', 'Mme', 'Ms' --> Miss
# 'Lady', 'Dona', 'Countess' --> Mrs
# 'Dr', 'Master', 'Major', 'Capt', 'Sir', 'Don' --> Mr
# 'Jonkheer' , 'Col' , 'Rev' --> Other
df = df.replace(['Mlle','Mme', 'Ms', 'Dr','Master','Major','Lady','Dona','Countess','Jonkheer','Col','Rev','Capt','Sir','Don'],
                ['Miss','Miss','Miss','Mr','Mr', 'Mr', 'Mrs',  'Mrs', 'Mrs', 'Other',  'Other','Other','Mr','Mr','Mr'])

