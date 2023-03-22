
# If You Want To Run A Job Which Is Linked To A Git Repo, The Service Principal Will Run The Job As It will Be Owner...
# ... The Service Principal, Without Receiving Git Authentication, Will Not Be Able To Access The Ropo Files For Which...
# ... The Job Needs.  


import requests 
import os
import json


def configureGit(gitConfig, workspaceId, databricksInstance, bearerToken, managementToken, SYSTEM_ACCESSTOKEN ):

    DBRKS_REQ_HEADERS  = {
        'Authorization': f'Bearer {bearerToken}',
        'X-Databricks-Azure-SP-Management-Token': f'{managementToken}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'{workspaceId}',
        'Content-Type': 'application/json'
    }

    newData = {
        "personal_access_token": SYSTEM_ACCESSTOKEN
        }
    
    gitConfig.update(newData)
    print(gitConfig)

    response = requests.post('https://' + databricksInstance + '/api/2.0/git-credentials', headers=DBRKS_REQ_HEADERS, json=gitConfig)

    if response.status_code != 200:

        credential = requests.get('https://' + databricksInstance + '/api/2.0/git-credentials', headers=DBRKS_REQ_HEADERS)
        print(f"Credential is {credential}")
        response = requests.patch('https://' + databricksInstance + '/api/2.0/git-credentials'+ credential, headers=DBRKS_REQ_HEADERS, json=gitConfig)
    
    print(response.json())

if __name__ == "__main__":

    with open('.github/MLOps_Engineer/Variables/' + os.environ['ENVIRONMENT'] +'/Repos.json', 'r') as f:
        json = json.load(f)

    gitConfigs = json['Git_Configuration']

    for gitConfig in gitConfigs:
        response = configureGit(gitConfig=gitConfig, 
                                workspaceId=os.environ['WORKSPACE_ID'], 
                                databricksInstance=os.environ['DATABRICKS_INSTANCE'], 
                                bearerToken=os.environ['DBRKS_BEARER_TOKEN'], 
                                managementToken=os.environ['DBRKS_MANAGEMENT_TOKEN'], 
                                SYSTEM_ACCESSTOKEN=os.environ['SYSTEM_ACCESSTOKEN'] )       