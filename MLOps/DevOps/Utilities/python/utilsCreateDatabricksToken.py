

from azure_databricks_sdk_python import Client, AuthMethods
import os


DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
DATABRICKS_TOKEN = os.environ['DATABRICKS_TOKEN']
DATABRICKS_MANAGEMENT_TOKEN = os.environ['DATABRICKS_MANAGEMENT_TOKEN']

#Client(auth_method=AuthMethods.AZURE_AD_SERVICE_PRINCIPAL, databricks_instance=DATABRICKS_INSTANCE, access_token=DBRKS_BEARER_TOKEN, management_token=DBRKS_MANAGEMENT_TOKEN, resource_id="<resource_id>")
client = Client(auth_method=AuthMethods.AZURE_AD_SERVICE_PRINCIPAL, databricks_instance=DATABRICKS_INSTANCE, access_token=DATABRICKS_TOKEN, management_token=DATABRICKS_MANAGEMENT_TOKEN)

print(client)
print(client.test_connection())
token = client.tokens.create(comment="A happy token from the docs.")
print(token)
print(token.token_info.comment)