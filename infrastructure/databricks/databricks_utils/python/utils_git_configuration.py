
# If You Want To Run A Job Which Is Linked To A Git Repo, The Service Principal Will Run The Job As It will Be Owner...
# ... The Service Principal, Without Receiving Git Authentication, Will Not Be Able To Access The Ropo Files For Which...
# ... The Job Needs.  


import requests 
import os
import json


def configureGit(gitConfig, workspaceId, databricksInstance, bearerToken, managementToken, githubToken, environment):

    DBRKS_REQ_HEADERS  = {
        'Authorization': f'Bearer {bearerToken}',
        'X-Databricks-Azure-SP-Management-Token': f'{managementToken}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'{workspaceId}',
        'Content-Type': 'application/json'
    }

    newData = {
        "personal_access_token": githubToken
        }
    
    gitConfig.update(newData)
    #print(gitConfig)
    #print(DBRKS_REQ_HEADERS)

    response = requests.post('https://' + databricksInstance + '/api/2.0/git-credentials', headers=DBRKS_REQ_HEADERS, json=gitConfig)
    print(response)
    #print(response.json())

    if response.status_code != 200:

        response = requests.get('https://' + databricksInstance + '/api/2.0/git-credentials', headers=DBRKS_REQ_HEADERS)
        #print(response.json())
        credential = response.json()["credentials"][0]["credential_id"]
        #print(f"Credential is {credential}")
        response = requests.patch('https://' + databricksInstance + '/api/2.0/git-credentials/'+ str(credential), headers=DBRKS_REQ_HEADERS, json=gitConfig)
    
    #print(response.json())

if __name__ == "__main__":

    with open('infrastructure/databricks/databricks_configs/' + os.environ['ENVIRONMENT'] +'/repos.json', 'r') as f:
        json = json.load(f)

    gitConfigs = json['Git_Configuration']
    print(gitConfigs)

    #print(os.environ['WORKSPACE_ID'])
    #print(os.environ['DATABRICKS_INSTANCE'])
    #print(os.environ['DATABRICKS_AAD_TOKEN'])
    #print(os.environ['DATABRICKS_MANAGEMENT_TOKEN'])
    #print(os.environ['PAT_GITHUB'])
    #print(os.environ['ENVIRONMENT'])
    for gitConfig in gitConfigs:
        response = configureGit(gitConfig=gitConfig, 
                                workspaceId=os.environ['WORKSPACE_ID'], 
                                databricksInstance=os.environ['DATABRICKS_INSTANCE'], 
                                bearerToken=os.environ['DATABRICKS_AAD_TOKEN'], 
                                managementToken=os.environ['DATABRICKS_MANAGEMENT_TOKEN'], 
                                githubToken=os.environ['PAT_GITHUB'], 
                                environment=os.environ['ENVIRONMENT'])       