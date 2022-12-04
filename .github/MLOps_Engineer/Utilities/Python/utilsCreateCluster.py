
import requests
import time
import os
import json

WORKSPACE_ID = os.environ['WORKSPACE_ID']
DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
DBRKS_BEARER_TOKEN = os.environ['DBRKS_BEARER_TOKEN']
DBRKS_MANAGEMENT_TOKEN = os.environ['DBRKS_MANAGEMENT_TOKEN']
ENVIRONMENT = os.environ['ENVIRONMENT']

DBRKS_REQ_HEADERS = {
    'Authorization': f'Bearer {DBRKS_BEARER_TOKEN}',
    'X-Databricks-Azure-SP-Management-Token': f'{DBRKS_MANAGEMENT_TOKEN}',
    'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
    'Content-Type': 'application/json'
}

def createCluster(postjson):
    """
        Takes Json object for cluster creation, and invokes the Databricks API.
    """

    response = requests.post(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/create', headers=DBRKS_REQ_HEADERS, json=postjson
    )

    if response.status_code != 200:
        raise Exception(response.text)

    os.environ["DBRKS_CLUSTER_ID"] = response.json()["cluster_id"]
    
    print( "##vso[task.setvariable variable=DBRKS_CLUSTER_ID;isOutput=true;]{b}".format( b=os.environ["DBRKS_CLUSTER_ID"]))


def listClusters():
    """
        Returns a Json object containing a list of existing Databricks Clusters.
    """

    response = requests.get('https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/list', headers=DBRKS_REQ_HEADERS)

    if response.status_code != 200:
        raise Exception(response.content)

    else:
        return response.json()


def get_dbrks_cluster_info():
    DBRKS_CLUSTER_ID = {'cluster_id': os.environ["DBRKS_CLUSTER_ID"]}

    response = requests.get('https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/get', headers=DBRKS_REQ_HEADERS, params=DBRKS_CLUSTER_ID)
    
    if response.status_code == 200:
        return json.loads(response.content)
    
    else:
        raise Exception(json.loads(response.content))


def manageClusterState():
    awaitCluster = True
    startedTerminatedCluster = False
    clusterRestarted = False
    startTime = time.time()
    loopTime = 1200  # 20 Minutes
    update_time = 30
    while awaitCluster:
        currentTime = time.time()
        elapsedTime = currentTime - startTime
        
        if elapsedTime > loopTime:
            raise Exception('Error: Loop took over {} seconds to run.'.format(loopTime))
        
        if get_dbrks_cluster_info()['state'] == 'TERMINATED':
            print('Starting Terminated Cluster')
            started_terminated_cluster = True
            raise ValueError("Failed to create cluster, cluster teminated")
        
        elif get_dbrks_cluster_info()['state'] == 'RESTARTING':
            print('Cluster is Restarting')
            time.sleep(60)
        
        elif get_dbrks_cluster_info()['state'] == 'PENDING':
            print('Cluster is Pending Start')
            time.sleep(60)
        
        else:
            print('Cluster is Running')
            
            awaitCluster = False



if __name__ == "__main__":
    with open('.github/MLOps_Engineer/Variables/' + ENVIRONMENT + '/DBX_Clusters/Clusters.json', 'r') as f:
        buildClusters = json.load(f)
    
    # Extract array from Json object
    buildClusters = buildClusters['Clusters']
    
    for buildCluster in buildClusters:
        existingClusters = listClusters()

        if existingClusters:
            for existingCluster in existingClusters['clusters']:
                if existingCluster['cluster_name'] == buildCluster['cluster_name']:
                    print("Cluster Exists - Do Nothing")
                else:
                    print("Cluster Does Not Exist -  Build")
                    
                    createCluster(buildCluster)
                    manageClusterState()
        else:
            print("There Are No Clusters - Build ")
            
            createCluster(buildCluster)
            manageClusterState()