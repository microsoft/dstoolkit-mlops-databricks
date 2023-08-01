import os
import subprocess
import requests

__here__ = os.path.dirname(__file__)

RESOURCE_GROUP_NAME = os.environ['RESOURCE_GROUP_NAME']
DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
WORKSPACE_ID = os.environ['WORKSPACE_ID']
SUBSCRIPTION_ID = os.environ['SUBSCRIPTION_ID']
DATABRICKS_AAD_TOKEN = os.environ['DATABRICKS_AAD_TOKEN']
DATABRICKS_MANAGEMENT_TOKEN = os.environ['DATABRICKS_MANAGEMENT_TOKEN']
ARM_CLIENT_ID = os.environ['ARM_CLIENT_ID']
ARM_CLIENT_SECRET = os.environ['ARM_CLIENT_SECRET']
ARM_TENANT_ID = os.environ['ARM_TENANT_ID']
AML_WS_NAME = os.environ['AML_WS_NAME']


DBRKS_REQ_HEADERS = {
    'Authorization': f'Bearer {DATABRICKS_AAD_TOKEN}',
    'X-Databricks-Azure-SP-Management-Token': f'{DATABRICKS_MANAGEMENT_TOKEN}',
    'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
    'Content-Type': 'application/json'
}



def run_cmd(cmd):
    #May Need To Rmove shell=True
    process = subprocess.run(cmd, stdout=subprocess.PIPE)
    output = process.stdout.decode().split('\n')
    #print(output)
    output = [line.strip('\n').strip('\r') for line in output]


    #print(f"Return Code: {process.returncode}")
    if process.returncode != 0:
        raise RuntimeError('\n'.join(output))
    return output

def get_app_insight_name():
    cmd = ["az", "resource", "list", "-g", RESOURCE_GROUP_NAME, "--resource-type", "microsoft.insights/components", "--query", "[].name", "-o", "tsv"]
    name = run_cmd(cmd)
    return name


def get_app_insight_key(name):
    cmd = ["az", "monitor", "app-insights", "component", "show", "-g", RESOURCE_GROUP_NAME, "-a", name, "--query", "connectionString", "-o", "tsv"]
    key = run_cmd(cmd)
    return key


def create_secret_scopes(scope_name=str, initial_manage_principal=str):
    """
        Takes Json object for cluster creation, and invokes the Databricks API.
    """
    postjson = {
        "scope": scope_name,
        "initial_manage_principal": initial_manage_principal
    }

    response = requests.post(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/secrets/scopes/create', headers=DBRKS_REQ_HEADERS, json=postjson
    )

    #print(response.status_code)
    #if response.status_code != 200:
    #    raise Exception(response.text)

    #print(response.json())

def insert_secret(secret_value=str, scope_name=str, key=str):
    """
        Takes Json object for cluster creation, and invokes the Databricks API.
    """
    postjson = {
        "scope": scope_name,
        "key": key,
        "string_value": secret_value
    }

    response = requests.post(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/secrets/put', headers=DBRKS_REQ_HEADERS, json=postjson
    )
    #print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.text)

    #print(response.json())
    

if __name__ == '__main__':
    app_insight_name = get_app_insight_name()[0]
    #print(app_insight_name)
    app_insight_key = get_app_insight_key(app_insight_name)[0]
    #print(app_insight_key)


    # Create Secret Scopes
    create_secret_scopes(scope_name="DBX_SP_Credentials", initial_manage_principal="users")
    create_secret_scopes(scope_name="AzureResourceSecrets", initial_manage_principal="users")

    # Insert Secrets into Secret Scope "DBX_SP_Credentials"
    insert_secret(secret_value=ARM_CLIENT_ID, scope_name="DBX_SP_Credentials", key="DBX_SP_Client_ID")
    insert_secret(secret_value=ARM_CLIENT_SECRET, scope_name="DBX_SP_Credentials", key="DBX_SP_Client_Secret")
    insert_secret(secret_value=ARM_TENANT_ID, scope_name="DBX_SP_Credentials", key="DBX_SP_Tenant_ID")
    insert_secret(secret_value=SUBSCRIPTION_ID, scope_name="DBX_SP_Credentials", key="SUBSCRIPTION_ID")

    # Insert Secrets into Secret Scope "AzureResourceSecrets"
    insert_secret(secret_value=app_insight_key, scope_name="AzureResourceSecrets", key="AppInsightsKey")
    insert_secret(secret_value=RESOURCE_GROUP_NAME, scope_name="AzureResourceSecrets", key="RESOURCE_GROUP_NAME")
    insert_secret(secret_value=AML_WS_NAME, scope_name="AzureResourceSecrets", key="AML_WS_NAME")
