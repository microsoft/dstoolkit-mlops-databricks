import os
import json
import time
from urllib.error import HTTPError

import requests

from dbx_utils.common import get_databricks_request_headers


def ingest_cluster_param_file(filename):

    """
        loads  the json parameter file containing
        databricks cluster configs to build

        :param: filename: param file location
        :type: str

        :return: cluster_param_file: dbx cluster definitions
        :type: str
    """
    with open(filename, 'r', encoding="utf-8") as file:
        cluster_param_file = json.load(file)
        cluster_param_file = cluster_param_file['Clusters']

    return cluster_param_file


def create_clusters():
    """
        Main script which calls sub functions to create
        databricks clusters degined in params file
    """

    environment = os.environ.get("ENVIRONMENT")

    cluster_param_file = ingest_cluster_param_file(
        'infrastructure/databricks/databricks_configs/'
        + environment +
        '/clusters.json'
    )

    existing_clusters, _ = list_existing_clusters()

    existing_clusters_name_arr = get_cluster_names(existing_clusters)

    for cluster in cluster_param_file:
        # Cluster Does Not Exist - Deploy
        if cluster['cluster_name'] not in existing_clusters_name_arr:
            cluster_status, cluster_id = create_cluster(cluster)
            print(f"Cluster Status: {cluster_status}")
            manage_cluster_state(cluster_id)
        else:
            print(
                f"Cluster {cluster['cluster_name']} already exists - Skipping."
            )


def list_existing_clusters():
    """
        Returns a Json object containing a list
        of existing Databricks Clusters.

        :return: response_content: dbx api response with clusters
        :type: str

        :return: status_code: api status code
        :type: int
    """

    databricks_req_headers = get_databricks_request_headers()
    databricks_instance = os.environ.get("DATABRICKS_INSTANCE")
    response = requests.get(
        'https://' + databricks_instance + '/api/2.0/clusters/list',
        headers=databricks_req_headers,
        timeout=10
    )

    if response.ok:
        return response.json(), response.status_code

    raise HTTPError(
        response.url, code=response.status_code, msg="Failure",
        hdrs=response.headers, fp=response
    )


def get_cluster_names(existing_clusters):
    """
        Parses JSON object with existing databricks clusters
        and returns an array with cluster names

        :param: cluster: json object of existing dbx clusters
        :type: str

        :return: existing_clusters_name_arr: array of cluster names
        :type: array
    """
    existing_clusters_name_arr = []

    if existing_clusters:  # If clusters exist
        for existing_cluster in existing_clusters['clusters']:
            existing_clusters_name_arr.append(existing_cluster['cluster_name'])
        return existing_clusters_name_arr
    # If No Clusters Exist, Return Empty Array
    return existing_clusters_name_arr


def create_cluster(cluster):
    """
        Takes json definitions for clusters to create,
        and invokes the Databricks  Cluster API.

        :param: cluster: cluster definition
        :type: str

        :return: status code: response status for api call
        :type: int

        :return: cluster_id: cluster id for newly created databricks cluster
        :type: str
    """
    databricks_req_headers = get_databricks_request_headers()
    databricks_instance = os.environ.get("DATABRICKS_INSTANCE")
    response = requests.post(
        'https://' + databricks_instance + '/api/2.0/clusters/create',
        headers=databricks_req_headers,
        json=cluster,
        timeout=10
    )

    if response.ok:
        cluster_id = response.json()["cluster_id"]
        return response.status_code, cluster_id

    raise HTTPError(
        response.text,
        code=response.status_code,
        msg="Failure",
        hdrs=response.headers,
        fp=response
    )


def manage_cluster_state(cluster_id):
    """
        Function contuninally checks cluster state until
        cluster is Running, or Fails to Start

        :param: cluster_id: clusterid for the Databricks Cluster
        :type: str
    """

    await_cluster = True
    start_time = time.time()
    loop_time = 1200  # 20 Minutes
    while await_cluster:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time > loop_time:
            raise Exception(f'Error: Loop took over {loop_time} seconds to run.')
        if get_databricks_cluster_info(cluster_id)['state'] == 'TERMINATED':
            print('Starting Terminated Cluster')
            raise ValueError("Failed to create cluster, cluster teminated")
        if get_databricks_cluster_info(cluster_id)['state'] == 'RESTARTING':
            print('Cluster is Restarting')
            time.sleep(60)
        elif get_databricks_cluster_info(cluster_id)['state'] == 'PENDING':
            print('Cluster is Pending Start')
            time.sleep(60)
        else:
            print('Cluster is Running')
            await_cluster = False


def get_databricks_cluster_info(cluster_id):
    """
        Returns an object revealing the Databricks Cluster State
        "Terminated", "Restarting", or "Pending"

        :param: cluster_id: clusterid for the Databricks Cluster
        :type: str

        :return: api response object
        :type: str
    """
    databricks_req_headers = get_databricks_request_headers()
    databricks_instance = os.environ.get("DATABRICKS_INSTANCE")
    databricks_cluster_id = {'cluster_id': cluster_id}

    response = requests.get(
        'https://' + databricks_instance + '/api/2.0/clusters/get',
        headers=databricks_req_headers,
        params=databricks_cluster_id,
        timeout=10
        )

    if response.ok:
        return response.json()

    raise HTTPError(
            response.text,
            code=response.status_code,
            msg="Failure",
            hdrs=response.headers,
            fp=response
        )


def main():
    """
        Main function to invoke cluster creation
    """

    create_clusters()


if __name__ == "__main__":
    main()
