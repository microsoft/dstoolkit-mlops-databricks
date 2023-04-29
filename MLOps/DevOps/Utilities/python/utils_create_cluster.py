# IMPORTANT - AZUREML_MLFLOW PYPI PACKAGE IS INSTALLED ONTO CLUSTER
# This allows us to use the MLFlow API to log metrics and artifacts \
# to the MLFlow Tracking Server in AML
"""
    Import Packages
"""
import os
import json
import time
import requests
import os
from utilsCreateAuthenticationTokens import createBearerToken, createManagementToken



WORKSPACE_ID = os.environ.get("WORKSPACE_ID")
DATABRICKS_INSTANCE = os.environ.get("DATABRICKS_INSTANCE")
DATABRICKS_AAD_TOKEN = os.environ.get("DATABRICKS_AAD_TOKEN")
DATABRICKS_MANAGEMENT_TOKEN = os.environ.get("DATABRICKS_MANAGEMENT_TOKEN")
ENVIRONMENT = os.environ.get("ENVIRONMENT")


print("Test")
print(f"WORKSPACE_ID {WORKSPACE_ID}")
print(f"DATABRICKS_INSTANCE {DATABRICKS_INSTANCE}")
print(f"DATABRICKS_AAD_TOKEN {DATABRICKS_AAD_TOKEN}")
print(f"DATABRICKS_MANAGEMENT_TOKEN {DATABRICKS_MANAGEMENT_TOKEN}")
print(f"ENVIRONMENT {ENVIRONMENT}")



DBRKS_REQ_HEADERS = {
    'Authorization': f'Bearer {DATABRICKS_AAD_TOKEN}',
    'X-Databricks-Azure-SP-Management-Token': f'{DATABRICKS_MANAGEMENT_TOKEN}',
    'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
    'Content-Type': 'application/json'
}


print(f"DBRKS_REQ_HEADERS {DBRKS_REQ_HEADERS}")
#WORKSPACE_ID = os.environ['WORKSPACE_ID']
#DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
#DATABRICKS_AAD_TOKEN = os.environ['DATABRICKS_AAD_TOKEN']
#DATABRICKS_MANAGEMENT_TOKEN = os.environ['DATABRICKS_MANAGEMENT_TOKEN']
#ENVIRONMENT = os.environ['ENVIRONMENT']


def _create_cluster(postjson):
    """
        Takes Json object for cluster creation, and invokes the Databricks API.
    """
    response = requests.post(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/create', 
        headers=DBRKS_REQ_HEADERS,
        json=postjson
    )

    if response.status_code != 200:
        raise Exception(response.text)

    cluster_id = response.json()["cluster_id"]
    print(cluster_id)
    return response.status_code, cluster_id


def _list_clusters():
    """
        Returns a Json object containing a list of existing Databricks Clusters.
    """

    response = requests.get('https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/list',
                            headers=DBRKS_REQ_HEADERS
                            )
    #import pdb; pdb.set_trace()
    if response.status_code != 200:
        raise Exception(response.content)
    return response.json(), response.status_code


def _get_dbrks_cluster_info(cluster_id):
    """
        Returns a Json object containing information about a specific Databricks Cluster.
    
    """

    tmp_cluster_id = cluster_id

    print(f"VITAL: custer_id {cluster_id}")
    response = requests.get('https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/get', headers=DBRKS_REQ_HEADERS, params=tmp_cluster_id)
    print(response.status_code)
    print(response.content)

    if response.status_code == 200:
        return json.loads(response.content)
    raise Exception(json.loads(response.content))


def _manage_cluster_state(cluster_id):
    """
        Returns a Json object containing information about the Cluster State
    """
    tmp_cluster_id = cluster_id
    await_cluster = True
    start_time = time.time()
    loop_time = 1200  # 20 Minutes
    while await_cluster:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time > loop_time:
            raise Exception('Error: Loop took over {} seconds to run.'.format(loop_time))
        if _get_dbrks_cluster_info(tmp_cluster_id)['state'] == 'TERMINATED':
            print('Starting Terminated Cluster')
            raise ValueError("Failed to create cluster, cluster teminated")
        elif _get_dbrks_cluster_info(tmp_cluster_id)['state'] == 'RESTARTING':
            print('Cluster is Restarting')
            time.sleep(60)
        elif _get_dbrks_cluster_info(tmp_cluster_id)['state'] == 'PENDING':
            print('Cluster is Pending Start')
            time.sleep(60)
        else:
            print('Cluster is Running')
            await_cluster = False


def ingest_json_parameters_file():
    """
        Ingests the Json Parameters File for Cluster Creation
    """
    with open('MLOps/DevOps/Variables/' + ENVIRONMENT + '/Clusters.json', 'r') as file:
        json_cluster_param_file = json.load(file)
        json_cluster_param_file = json_cluster_param_file['Clusters']
        return json_cluster_param_file


def list_existing_clusters():
    """
        Returns a list of existing clusters
    """
    existing_clusters, return_code = _list_clusters()
    tmp_arr = []
    if existing_clusters:
        for existing_cluster in existing_clusters['clusters']:
            tmp_arr.append(existing_cluster['cluster_name'])
    return tmp_arr


def cluster_check(arr_obj, cluster):
    """
        The Logic Will Determine Whether A Cluster Exists Or Not.
        Returns True When Conditions Satisified To Deploy Cluster 
    
    """
    if arr_obj:
        if cluster['cluster_name'] not in arr_obj:
            return True # Cluster Exists: Don't Do Anything
        return False
    return True # No Clusters Exist At All: Build

def create_api_headers():
    dbkrs_req_headers = {
        'Authorization': f'Bearer {DATABRICKS_AAD_TOKEN}',
        'X-Databricks-Azure-SP-Management-Token': f'{DATABRICKS_MANAGEMENT_TOKEN}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
        'Content-Type': 'application/json'
        }
    
    return dbkrs_req_headers

def main():
    json_cluster_param_file = ingest_json_parameters_file()
    print(json_cluster_param_file)
    for cluster in json_cluster_param_file:
        existing_clusters_arr = list_existing_clusters()
        print(f"existing_clusters_arr {existing_clusters_arr}")
        deploy_cluster_bool = cluster_check(existing_clusters_arr, cluster)
        print(f"deploy_cluster_bool {deploy_cluster_bool}")
        if deploy_cluster_bool:
            response, cluster_id = _create_cluster(cluster)
            print(f"response {response}")
            print(f"cluster_id {cluster_id}")
            _manage_cluster_state(cluster_id)

if __name__ == "__main__":
    main()
