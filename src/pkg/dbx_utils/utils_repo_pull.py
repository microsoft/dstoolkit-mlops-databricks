import requests
import os
import json
from dotenv import load_dotenv


load_dotenv(".env") # load environment variables


def _ingest_repo_param_file(filename):
    """
        Ingests the Json Parameters File for Repo Pull
    """
    with open(filename, 'r') as file:
        
        repo_param_file = json.load(file)['Repo_Configuration']        
        
        return repo_param_file


def get_repos_with_management_permissions():
    """
        Invokes Databricks API to get all repos with management permissions
    """

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

    response = requests.get(
    'https://' + DATABRICKS_INSTANCE + '/api/2.0/repos', headers=DBRKS_REQ_HEADERS
    )

    status_code = response.status_code
    repos_with_management_permissions = response.json()

    if response.status_code != 200:
        raise Exception(response.status_code)
    else:
        repos_with_management_permissions = repos_with_management_permissions['repos']
        return repos_with_management_permissions, status_code


def update_repo(repo_id, update_branch):
    """
        Invoked Databricks API to update repo
    """

    repo_id = str(repo_id)

    WORKSPACE_ID = os.environ.get("WORKSPACE_ID")
    DATABRICKS_INSTANCE = os.environ.get("DATABRICKS_INSTANCE")
    DATABRICKS_AAD_TOKEN = os.environ.get("DATABRICKS_AAD_TOKEN")
    DATABRICKS_MANAGEMENT_TOKEN = os.environ.get("DATABRICKS_MANAGEMENT_TOKEN")
    DATABRICKS_MANAGEMENT_TOKEN = os.environ.get("DATABRICKS_MANAGEMENT_TOKEN")
    
    DBRKS_REQ_HEADERS = {
        'Authorization': f'Bearer {DATABRICKS_AAD_TOKEN}',
        'X-Databricks-Azure-SP-Management-Token': f'{DATABRICKS_MANAGEMENT_TOKEN}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
        'Content-Type': 'application/json'
    }

    postjson = {
        "branch": str(update_branch)
        }
    
    print("Updated Repo Json String")
    print(postjson)

    response = requests.patch(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/repos/'+ repo_id, headers=DBRKS_REQ_HEADERS, json=postjson
    )

    if response.status_code != 200:
        raise Exception(response.content)
    else:
        #print(f"Status Code: {response.status_code}")
        #print(response.json())
        return response.status_code
  

def main():

    ENVIRONMENT = os.environ.get("ENVIRONMENT")

    file_name = 'infrastructure/databricks/databricks_configs/' + ENVIRONMENT + '/repos.json'
    repo_param_file = _ingest_repo_param_file(file_name)

    print(f"Repos To Connect {repo_param_file}")

    repos_with_management_permissions, status_code = get_repos_with_management_permissions()
    
    for repo in repo_param_file:

  
        update_folder = repo['path']
        update_branch = repo['branch']
        
        for item in repos_with_management_permissions:
            print(f" The Update Folder is {update_folder} and path is  {item['path']}")
            
            if update_folder in item['path']:
                print(f" The Update Folder {update_folder} is Contained within the Path {item['path']}")
                print("Retrieve the Repo ID")

                repo_id = str(item['id'])

                #Update repo
                #import pdb; pdb.set_trace()
                status_code = update_repo(repo_id, update_branch)
    
    return status_code


if __name__ == "__main__":
    main()




