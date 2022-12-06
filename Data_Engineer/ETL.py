# Databricks notebook source
# DATABRICKS SP NEEDS TO HAVE BLOB STORAGE CONTRIBUTOR ACCESS TO THE CONTAINER (OR POSSIBLY JUST THE SA)

import numpy as np
import pandas as pd 
from pyspark.sql.functions import *
#from databricks import feature_store
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
print(spark.version)
print('spark session created.')

# COMMAND ----------

#dbutils = DBUtils(spark)
#print(dbutils.fs.ls('dbfs:/FileStore/'))
#print(dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret"))
#dbutils.fs.mounts()
# IMPORTANT : https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#authenticate-via-visual-studio-code
#from azure.identity import DefaultAzureCredential
#from azure.storage.blob import BlobServiceClient
#from azure.storage.blob import ContainerClient
#from azure.keyvault.secrets import SecretClient
#default_credential = DefaultAzureCredential()
#client = SecretClient(vault_url="https://keyvault-dev-gayt.vault.azure.net", credential=default_credential)

# COMMAND ----------

# Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
account_url = "https://adlsdevgayt.blob.core.windows.net"
STORAGE_ACC_NAME = "adlsdevgayt"
DBX_SP_Client_Secret = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret")
DBX_SP_ClientID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_ClientID")
DBX_SP_TenantID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_TenantID")

print(f"Test: {DBX_SP_ClientID}")
print(f"Test: {DBX_SP_Client_Secret}")
print(DBX_SP_TenantID)

# COMMAND ----------

# Ensure container access is set to public access
# Ensure that RABC Blob contributor is set onto the container for the SP????

spark.conf.set("fs.azure.account.auth.type."+ STORAGE_ACC_NAME +".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+ STORAGE_ACC_NAME +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+ STORAGE_ACC_NAME +".dfs.core.windows.net", DBX_SP_ClientID )
spark.conf.set("fs.azure.account.oauth2.client.secret."+ STORAGE_ACC_NAME +".dfs.core.windows.net", DBX_SP_Client_Secret )   
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ STORAGE_ACC_NAME +".dfs.core.windows.net", "https://login.microsoftonline.com/"+ DBX_SP_TenantID + "/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")


print("Show Data in Bronze Container")
display(dbutils.fs.ls("abfss://bronze@"+ STORAGE_ACC_NAME +".dfs.core.windows.net"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")


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

display(dbutils.fs.ls("abfss://bronze@"+ STORAGE_ACC_NAME +".dfs.core.windows.net"))


dbfs_path1 = 'abfss://bronze@adlsdevgayt.dfs.core.windows.net/winequality-red.csv'
dbfs_path2 = 'abfss://bronze@adlsdevgayt.dfs.core.windows.net/winequality-white.csv'
mount_point1 = "/mnt/white_wine"
mount_point2 = "/mnt/red_wine"

if all(mount.mountPoint != mount_point1 for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source = dbfs_path1, mount_point = mount_point1, extra_configs = configs)
    
if all(mount.mountPoint != mount_point2 for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source = dbfs_path1, mount_point = mount_point2, extra_configs = configs)

display(dbutils.fs.mounts())

white_wine = spark.read.csv(mount_point1, header = "True", inferSchema="True", sep=';').toPandas()
display(white_wine)


red_wine = spark.read.csv(mount_point1, header = "True", inferSchema="True", sep=';').toPandas()
display(red_wine)


# COMMAND ----------

# MAGIC %md
# MAGIC # Read in CSV as Data Frame From Mount Point

# COMMAND ----------

red_wine['is_red'] = 1
white_wine['is_red'] = 0
 
data = pd.concat([red_wine, white_wine], axis=0)
 
# Remove spaces from column names
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Visualise The Data

# COMMAND ----------

import seaborn as sns
sns.distplot(data.quality, kde=False)

# COMMAND ----------

high_quality = (data.quality >= 7).astype(int)
data.quality = high_quality

# COMMAND ----------

import matplotlib.pyplot as plt
 
dims = (3, 4)
 
f, axes = plt.subplots(dims[0], dims[1], figsize=(25, 15))
axis_i, axis_j = 0, 0
for col in data.columns:
  if col == 'is_red' or col == 'quality':
    continue # Box plots cannot be used on indicator variables
  sns.boxplot(x=high_quality, y=data[col], ax=axes[axis_i, axis_j])
  axis_j += 1
  if axis_j == dims[1]:
    axis_i += 1
    axis_j = 0

