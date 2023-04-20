# TESTING STILL REQUIRED - DO NOT USE

import requests
import os
import json

WORKSPACE_ID = os.environ['WORKSPACE_ID']
DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
DATABRICKS_AAD_TOKEN = os.environ['DATABRICKS_AAD_TOKEN']
DATABRICKS_MANAGEMENT_TOKEN = os.environ['DATABRICKS_MANAGEMENT_TOKEN']
ENVIRONMENT = os.environ['ENVIRONMENT']
PULL_BRANCH = os.environ['PULL_BRANCH']



print(WORKSPACE_ID)
print(DATABRICKS_INSTANCE)
print(DATABRICKS_AAD_TOKEN)
print(DATABRICKS_MANAGEMENT_TOKEN)
print(PULL_BRANCH)


DBRKS_REQ_HEADERS = {
    'Authorization': f'Bearer {DATABRICKS_AAD_TOKEN}',
    'X-Databricks-Azure-SP-Management-Token': f'{DATABRICKS_MANAGEMENT_TOKEN}',
    'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
    'Content-Type': 'application/json'
}

def Get_Repos_With_Management_Permissions():

    response = requests.get(
    'https://' + DATABRICKS_INSTANCE + '/api/2.0/repos', headers=DBRKS_REQ_HEADERS
    )

    if response.status_code != 200:
        raise Exception(response.content)
    else:
        dict = response.json()
        array = dict['repos']
        return array

def Update_Repo(Repo_ID, Update_Branch):
    """
        Takes Json object for cluster creation, and invokes the Databricks API.
    """

    postjson = {
        "branch": str(Update_Branch)
        }
    

    print("Updated Repo Json String")
    print(postjson)



    response = requests.patch(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/repos/'+ Repo_ID, headers=DBRKS_REQ_HEADERS, json=postjson
    )

    print(response.json())
    


if __name__ == "__main__":
    ENVIRONMENT = os.environ['ENVIRONMENT']
    print(ENVIRONMENT)
    with open('AdvancedCacheManagementService/Build/parameters/' + ENVIRONMENT + '/Repos.json', 'r') as f:
        Repos_Config = json.load(f)
    
    # Extract array from Json object
    Repos_Config = Repos_Config['Repo_Configuration']

    print(f"Repos To Connect {Repos_Config}")
    Repos_With_Management_Permissions = Get_Repos_With_Management_Permissions()
    for Repo in Repos_Config:
        print(f"Repo {Repo}")
        Update_Folder = Repo['path']
        Update_Branch = Repo['branch']

        print(Update_Folder)

        
        print(f"Array of {Repos_With_Management_Permissions}")
        
        for Repo_With_Management_Permissions in Repos_With_Management_Permissions:
            print(f" The Update Folder is {Update_Folder} and path is  {Repo_With_Management_Permissions['path']}")
            if Update_Folder in Repo_With_Management_Permissions['path'] :
                print(f" The Update Folder {Update_Folder} is Contained within the Path {Repo_With_Management_Permissions['path']}")
                print("Retrieve the Repo ID")

                Repo_ID = str(Repo_With_Management_Permissions['id'])

                print(f" The RepoID is {Repo_ID}")

                print(f" Git Pull on DBX Repo {Update_Folder} with {PULL_BRANCH} Branch")

                repsonse = Update_Repo(Repo_ID, Update_Branch)

                print(repsonse)




