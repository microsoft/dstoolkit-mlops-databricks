# TESTING STILL REQUIRED - DO NOT USE


import requests
import time
import os
import json

WORKSPACE_ID = os.environ['WORKSPACE_ID']
DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
DATABRICKS_AAD_TOKEN = os.environ['DATABRICKS_AAD_TOKEN']
DATABRICKS_MANAGEMENT_TOKEN = os.environ['DATABRICKS_MANAGEMENT_TOKEN']
ENVIRONMENT = os.environ['ENVIRONMENT']
ARM_CLIENT_ID = os.environ['ARM_CLIENT_ID']

print(WORKSPACE_ID)
print(DATABRICKS_INSTANCE)
print(DATABRICKS_AAD_TOKEN)
print(DATABRICKS_MANAGEMENT_TOKEN)
print(ARM_CLIENT_ID)

DBRKS_REQ_HEADERS = {
    'Authorization': f'Bearer {DATABRICKS_AAD_TOKEN}',
    'X-Databricks-Azure-SP-Management-Token': f'{DATABRICKS_MANAGEMENT_TOKEN}',
    'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
    'Content-Type': 'application/json'
}


def Create_Repo(postjson):
    """
        Takes Json object for cluster creation, and invokes the Databricks API.
    """
    path = postjson['path']

    newData = {
        "path": "/Repos/"+ ARM_CLIENT_ID + "/" + path 
        }
    
    postjson.update(newData)

    print("Updated Repo Json String")
    print(postjson)

    response = requests.post(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/repos', headers=DBRKS_REQ_HEADERS, json=postjson
    )

    print(response.json())
    


if __name__ == "__main__":
    ENVIRONMENT = os.environ['ENVIRONMENT']
    print(ENVIRONMENT)
    with open( 'MLOps/DevOps/Variables/' + ENVIRONMENT + '/Repos.json', 'r') as f:
        Repos_Config = json.load(f)
    
    # Extract array from Json object
    Repos_Config = Repos_Config['Repo_Configuration']

    print(f"Repos To Connect {Repos_Config}")
    
    for Repo in Repos_Config:
        print(f"Repo {Repo}")
        Create_Repo(Repo)


