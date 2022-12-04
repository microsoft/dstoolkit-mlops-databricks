
import requests 
import os
import json

def listClusters(tokenRequestHeaders, databricksInstance):
    """
        Returns a Json object containing a list of existing Databricks Clusters.
    """

    response = requests.get('https://' + databricksInstance + '/api/2.0/clusters/list', headers=tokenRequestHeaders)

    if response.status_code != 200:
        raise Exception(response.content)

    else:
        return response.json()


def listJobs(tokenRequestHeaders, databricksInstance):
    """
        Returns a Json object containing a list of existing Databricks Jobs.
    """

    response = requests.get('https://' + databricksInstance + '/api/2.0/jobs/list', headers=tokenRequestHeaders)

    if response.status_code != 200:
        raise Exception(response.content)
    
    else:
        return response.json()


def createJobs(tokenRequestHeaders, existingClusters, buildJob, existingJobs, databricksInstance ):    
    """
        Creates Databricks Job
    """

    if existingClusters:
        
        for existingCluster in existingClusters['clusters']:
            
            if existingCluster['cluster_name'] == buildJob['cluster_name']:
                print("Cluster Exists - Retrieve Cluster ID")
                
                clusterId = existingCluster['cluster_id']

                newData = {
                    "existing_cluster_id": clusterId
                    }
                buildJob.update(newData)
        
                if existingJobs:
                    print("Jobs Already Exists - Update It")
                    response = requests.post('https://' + databricksInstance + '/api/2.1/jobs/reset', headers=tokenRequestHeaders, json=buildJob)

                else:
                    print("Job does not exist - Create from new")
                    response = requests.post('https://' + databricksInstance + '/api/2.1/jobs/create', headers=tokenRequestHeaders, json=buildJob)

                    if response.status_code != 200:
                        raise Exception(response.content)
                    
                    else:
                        return response.json()
            
            else:
                print("The Job Definition Contains A Cluster Which Does Not Exist")
    
    else:
        print("No Clusters Exist - Unable To Create Jobs")


if __name__ == "__main__":

    with open('MLOps_Engineer/1-DevOps_Pipeline_Layer/Variables/'+  os.environ['Environment'] +'/DBX_Jobs/Jobs.json', 'r') as f:
        buildJobs = json.load(f)


    WORKSPACE_ID = os.environ['WORKSPACE_ID']
    DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
    DBRKS_BEARER_TOKEN = os.environ['DBRKS_BEARER_TOKEN']
    DBRKS_MANAGEMENT_TOKEN = os.environ['DBRKS_MANAGEMENT_TOKEN']
    ENVIRONMENT = os.environ['Environment']

    tokenRequestHeaders = {
        'Authorization': f'Bearer {DBRKS_BEARER_TOKEN}',
        'X-Databricks-Azure-SP-Management-Token': f'{DBRKS_MANAGEMENT_TOKEN}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
        'Content-Type': 'application/json'
    }

    # Extract array from Json object
    buildJobs = buildJobs['Jobs']
    existingClusters = listClusters(tokenRequestHeaders, databricksInstance=DATABRICKS_INSTANCE)
    print(existingClusters)
    existingJobs = listJobs(tokenRequestHeaders, databricksInstance=DATABRICKS_INSTANCE)
    print(existingJobs)
    
    for buildJob in buildJobs:
        print(existingClusters)
        print(existingJobs)
        response = createJobs(buildJob=buildJob, existingClusters=existingClusters, existingJobs=existingJobs, databricksInstance=DATABRICKS_INSTANCE, tokenRequestHeaders=tokenRequestHeaders)
        print(response)         