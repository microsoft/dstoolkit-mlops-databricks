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
from dotenv import load_dotenv


load_dotenv(".env") # load environment variables


def _ingest_cluster_param_file(filename):
    """
        Ingests the Json Parameters File for Cluster Creation
    """
    with open(filename, 'r') as file:
        
        cluster_param_file = json.load(file)
        cluster_param_file = cluster_param_file['Clusters']
        return cluster_param_file


def create_clusters():   
    ENVIRONMENT = os.environ.get("ENVIRONMENT")
    cluster_param_file = _ingest_cluster_param_file('infrastructure/databricks/databricks_configs/' + ENVIRONMENT + '/clusters.json') 
    existing_clusters, _ = _list_existing_clusters()
    existing_clusters_name_arr = _get_cluster_names(existing_clusters)
    #print(existing_clusters_name_arr)
    for cluster in cluster_param_file:
        if cluster['cluster_name'] not in existing_clusters_name_arr:
            print(f"Cluster {cluster} does not exist - Deploy.")
            cluster_status, cluster_id = _create_cluster(cluster)
            print(f"Cluster {cluster_id} has been created.")
            print(f"Cluster Status: {cluster_status}")
            _manage_cluster_state(cluster_id)
        else:
            print(f"Cluster {cluster['cluster_name']} already exists - Skipping.")


def _list_existing_clusters():
    """
        Returns a Json object containing a list of existing Databricks Clusters.
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

    response = requests.get("https://" + DATABRICKS_INSTANCE + "/api/2.0/clusters/list",
                            headers=DBRKS_REQ_HEADERS
                            )
    status_code = response.status_code
    
    response_content = response.json()
    #print(response_content)

    if status_code != 200:
        raise Exception(status_code)
    else:
        print(f"Status Code: {status_code}")
        return response_content, status_code


def _get_cluster_names(existing_clusters):
        existing_clusters_name_arr = []
        
        
        if existing_clusters: # If Clusters Exist (Array Is Not Empty), Return Cluster Names
            for existing_cluster in existing_clusters['clusters']:
                
                existing_clusters_name_arr.append(existing_cluster['cluster_name'])  
            return existing_clusters_name_arr
        else: # If No Clusters Exist, Return Empty Array
            return existing_clusters_name_arr
    

def _create_cluster(cluster):
    """
        Takes Json object for cluster creation, and invokes the Databricks API.
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

    response = requests.post(
        'https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/create', 
        headers=DBRKS_REQ_HEADERS,
        json=cluster
    )

    print(response.status_code)

    if response.status_code != 200:
        #import pdb; pdb.set_trace()
        raise Exception(response.text)

    cluster_id = response.json()["cluster_id"]

    return response.status_code, cluster_id


def _manage_cluster_state(cluster_id):
    """
        Returns a Json object containing information about the Cluster State
    """
    
    await_cluster = True
    start_time = time.time()
    loop_time = 1200  # 20 Minutes
    while await_cluster:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time > loop_time:
            raise Exception('Error: Loop took over {} seconds to run.'.format(loop_time))
        if _get_databricks_cluster_info(cluster_id)['state'] == 'TERMINATED':
            print('Starting Terminated Cluster')
            raise ValueError("Failed to create cluster, cluster teminated")
        elif _get_databricks_cluster_info(cluster_id)['state'] == 'RESTARTING':
            print('Cluster is Restarting')
            time.sleep(60)
        elif _get_databricks_cluster_info(cluster_id)['state'] == 'PENDING':
            print('Cluster is Pending Start')
            time.sleep(60)
        else:
            print('Cluster is Running')
            await_cluster = False


def _get_databricks_cluster_info(cluster_id):
    """
        Returns a Json object containing information about a specific Databricks Cluster.
    
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
    DBRKS_CLUSTER_ID = {'cluster_id': cluster_id}

    response = requests.get('https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/get', headers=DBRKS_REQ_HEADERS, params=DBRKS_CLUSTER_ID)
    
    if response.status_code == 200:
        return response.json()
    raise Exception(json.loads(response.content))


def main():
    create_clusters()


if __name__ == "__main__":
    main()