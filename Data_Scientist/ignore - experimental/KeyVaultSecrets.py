# Databricks notebook source
print("test2")

# COMMAND ----------

# test

print("Testing")
a =[1,2]
for i in a:
    print(i)

# COMMAND ----------

from azure.identity import ClientSecretCredential

DBX_SP_Client_Secret = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_Client_Secret")
DBX_SP_ClientID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_ClientID")
DBX_SP_TenantID = dbutils.secrets.get(scope="DBX_SP_Credentials",key="DBX_SP_TenantID")


#credential = ClientSecretCredential(DBX_SP_TenantID, DBX_SP_ClientID, DBX_SP_Client_Secret )

# The keyvault secrets modules have a lot of conflicts, making it difficult to install
from azure.identity import DefaultAzureCredential
from azure.keyvault.keys import KeyClient
from azure.keyvault.secrets import SecretClient


# Running From VS Code - Ensure you are Sign in via Azure in VS Code
# DBX Execute - If the PAT Token was generated by SP (DevOps Agent), then you will be authenticating as SP
# and therefore require key vault admin permission on the KV 
#
credential = DefaultAzureCredential()

secret_client = SecretClient(vault_url="https://keyvault-dev-gayt.vault.azure.net/", credential=credential)
#secret = secret_client.set_secret("token", "secret")

secret = secret_client.get_secret("ARMCLIENTID")

print(secret.name)
print(secret.value)
