# Databricks notebook source
"""Build and installs the dbkframework."""

### I ADDED
import sys
import os



for path in sys.path:
    print(path)

####


from pathlib import Path

import sys
#print(sys.path)
#sys.path.append(str(Path(__file__).parent.parent.joinpath('modules')))
import os
import json
from dbkcore.core import Log
from dbkenv.core import ResourceClient
from dbkenv.core import Configuration
from dbkenv.core import DatabricksResourceManager
import argparse

Log(name=Path(__file__).stem)


def command_exec(command, ignore=False):
    """
    Execute shell command.

    Parameters
    ----------
    command : str
        Command to execute
    ignore : bool, optional
        Ignore exception, by default False

    Raises
    ------
    Exception
        Raises exception if command failes
    """
    Log.get_instance().log_info(f'Running command -> {command}')
    if not ignore:
        if os.system(command) != 0:
            raise Exception(f'Failed to execute: {command}')


def parse_args(args_list=None):
    """
    Parse command line arguments.

    Parameters
    ----------
    args_list : [type], optional
        Argument list, by default None

    Returns
    -------
    ArgumentParser
        Arguments parsed
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', help="Full path of cluster's json configuration", type=str, required=True)
    args_parsed = parser.parse_args(args_list)
    return args_parsed


def main(cluster_config_file):
    """
    Execute the script.

    Parameters
    ----------
    cluster_config_file : str
        Path of the configuration file

    Raises
    ------
    Exception
        Raises when script failes
    """

    # If you are working locally, then upload .env file --> Change file_load=True --> Also ensure from dotenv import load_dotenv
    configuration = Configuration(file_load=False)
    with open(cluster_config_file.strip(), 'r') as cl:
        cluster_configuration = json.load(cl)

    cluster_name = cluster_configuration['cluster_name']

    print("Databricks Host And Databricks Token: For Resource Client Object Creation")
    print(configuration.DATABRICKS_HOST)
    print(configuration.DATABRICKS_TOKEN)

    
    #This is Actual API Client i.e client
    client = ResourceClient(
        host=configuration.DATABRICKS_HOST,
        personal_token=configuration.DATABRICKS_TOKEN
    )

    db = client.apiClient.cluster.list_clusters(headers=None)

    print("API Client")
    print(client)

    #next add .cluster

    #client.cluster


    #print(client.host)
    #print(client.personal_token)
    #print(client.apiClient)    
    #db = client.apiClient
    #print(db.cluster.list_clusters(headers=None))
    #print(client.apiClient)



    # client.apiClient.
    drm = DatabricksResourceManager(
        client=client,
        cluster_name=cluster_name,
        cluster_configuration=cluster_configuration
    )

    print("Databricks Resource Manager")
    print(drm)

 

    #####

    print("ClusterID")
    cluster_id = drm.cluster.cluster_id
    print(cluster_id)
 
 

    drm.cluster.start_cluster_and_wait()

    modules_to_deploy = [
        'dbkframework'
    ]

    pipelines_folder = Path(__file__).\
        parent.\
        parent.\
        parent.\
        absolute().\
        joinpath('pipelines')
    
    print("Pipelines Folder")
    print(pipelines_folder)

    for module in modules_to_deploy:
        print("Module")
        print(module)

        package_folder = pipelines_folder.joinpath(module)
        print("Package Folder")
        print(package_folder)

        dist_folder = package_folder.joinpath('dist')
        print("Dist Folder")
        print(dist_folder)

        setup_file = package_folder.joinpath('setup.py')
        print("Set Up File")
        print(setup_file)

        command_string = f"cd {str(package_folder)} && python {str(setup_file)} sdist bdist_wheel"
        print(command_string)
        

        res = os.system(command_string)

        # NEW .
        #res = os.system(f'cd {str(package_folder)} && python {str(setup_file)} sdist bdist_wheel') 


        #####
        print("Print Result")
        print(res)

        if res != 0:
            raise Exception(f'Failed to build {module}')

        wheel = sorted([v for v in dist_folder.glob('*.whl')], key=lambda i: i.stat().st_ctime, reverse=True)[0]
        dbk_whl_name = wheel.name
        dbk_whl_root = 'dbfs:/FileStore/dev/artifacts/'
        dbk_whl_path = f'{dbk_whl_root}{dbk_whl_name}'




        command_exec(f'databricks fs -h')
        command_exec(f'databricks fs ls')

        command_exec(f'databricks fs rm {dbk_whl_root}', ignore=True)
        command_exec(f'databricks fs cp -r {wheel} {dbk_whl_path}')

        command_exec(f'databricks libraries uninstall --cluster-id {cluster_id} --whl {dbk_whl_path}')
        command_exec(f'databricks libraries install --cluster-id {cluster_id} --whl {dbk_whl_path}')

    command_exec(f'databricks clusters restart --cluster-id {cluster_id}')


if __name__ == "__main__":
    args = parse_args()
    main(cluster_config_file=args.config_file)
