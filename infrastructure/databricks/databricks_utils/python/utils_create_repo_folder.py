# TESTING STILL REQUIRED - DO NOT USE


import requests
import time
import os
import json
from dotenv import load_dotenv


load_dotenv(".env") # load environment variables

def _ingest_repo_param_file(filename):
    """
        Ingests the Json Parameters File for Databricks Repo Creation
    """
    with open(filename, 'r') as file:
        
        repo_param_file = json.load(file)['Repo_Configuration']        
        
        return repo_param_file
    
def create_databricks_repos(postjson):
    """
        Takes Json object for cluster creation, and invokes the Databricks API.
    """

    ARM_CLIENT_ID = os.environ.get("ARM_CLIENT_ID")
    WORKSPACE_ID = os.environ.get("WORKSPACE_ID")
    DATABRICKS_INSTANCE = os.environ.get("DATABRICKS_INSTANCE")
    DATABRICKS_AAD_TOKEN = os.environ.get("DATABRICKS_AAD_TOKEN")
    DATABRICKS_MANAGEMENT_TOKEN = os.environ.get("DATABRICKS_MANAGEMENT_TOKEN")
    

    DBRKS_REQ_HEADERS = {
        'Authorization': f'Bearer {DATABRICKS_AAD_TOKEN}',
        'X-Databricks-Azure-SP-Management-Token': f'{DATABRICKS_MANAGEMENT_TOKEN}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
        'Content-Type': 'application/json'
    }

    path = postjson['path']
    #import pdb; pdb.set_trace()

    newData = {
        "path": "/Repos/"+ ARM_CLIENT_ID + "/" + path 
        }
    
    postjson.update(newData)

    print("Updated Repo Json String")
    print(postjson)

    response = requests.post(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/repos', headers=DBRKS_REQ_HEADERS, json=postjson
    )

    #400: Already Exists
    print(f"Response: {response.content}")

    if response.status_code == 200 or response.status_code == 400:
        print(f"Status Code: {response.status_code}")
    else:
        raise Exception(response.status_code)


def main():

    ENVIRONMENT = os.environ.get("ENVIRONMENT")

    file_name = 'infrastructure/databricks/databricks_configs/' + ENVIRONMENT + '/repos.json'
    repo_param_file = _ingest_repo_param_file(file_name)

    # Extract array from Json object

    print(f"Repos To Connect {repo_param_file}")

    for repo in repo_param_file:
        print(f"Repo {repo}")
        create_databricks_repos(repo)


if __name__ == "__main__":
    main()

    



