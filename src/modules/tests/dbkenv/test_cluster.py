import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from dbkcore.core import Log
from dbkenv.core import ResourceClient
from dbkenv.core import Configuration
from dbkenv.core import DatabricksResourceManager
from dbkenv.local import DatabricksLocal
import json
import os
import pytest

# import time


def clients():
    configuration = Configuration(file_load=True)
    cluster_config_file = str(Path(__file__).parent.joinpath('unittest_cluster.json'))

    with open(cluster_config_file, 'r') as cl:
        cluster_configuration = json.load(cl)

    cluster_name = cluster_configuration['cluster_name']
    # instantiate the logger
    Log(
        name='unittest',
        connection_string=configuration.APPINSIGHT_CONNECTIONSTRING
    )
    client = ResourceClient(
        host=configuration.DATABRICKS_HOST,
        personal_token=configuration.DATABRICKS_TOKEN
    )
    drm = DatabricksResourceManager(
        client=client,
        cluster_name=cluster_name,
        cluster_configuration=cluster_configuration
    )

    return drm


def test_cluster_create():
    assert clients().cluster.create_cluster_and_wait(), "Cluster not created"


# def test_cluster_start():
#     assert clients().cluster.cluster_started(), "Failed to start cluster"


def test_local_dev():
    configuration = Configuration(file_load=True)
    dbc = DatabricksLocal(
        host=configuration.DATABRICKS_HOST,
        databricks_token=configuration.DATABRICKS_TOKEN,
        cluster_id=clients().cluster.cluster_id,
        org_id=configuration.DATABRICKS_ORDGID
    )
    success = dbc.initialize()
    assert success, "Failed to configure locally"


# Test content
source_file_name = 'unittest_notebook.py'
source_file_path = str(Path(__file__).parent.joinpath('content', source_file_name))
with open(source_file_path, 'r') as file:
    data = file.read()

destination_dir = "/unittesting"
destination_file_path = os.path.join(destination_dir, source_file_name)


def test_file_upload():
    clients().workspace.make_dir(destination_dir)
    clients().workspace.upload_content(destination_file_path, data)
    content = clients().workspace.list_content(destination_folder=destination_file_path)
    elements_in_folder = [os.path.basename(e["path"]) for e in content['objects']]
    assert source_file_name in elements_in_folder, "Failed to upload the file"


def test_file_run():
    output = clients().run_notebook_and_wait(
        destination_path=destination_file_path,
        delete_run=True
    )
    assert output == "success", "Failed to upload and run notebook"


def test_file_delete():
    content = clients().workspace.list_content(destination_folder=destination_dir)
    if not content:
        pytest.skip("Folder is empty")
    elif source_file_name not in [os.path.basename(e["path"]) for e in content['objects']]:
        pytest.skip("File not in folder")

    clients().workspace.delete_content(destination_file_path)
    content = clients().workspace.list_content(destination_folder=destination_dir)
    elements_in_folder = []
    if content: 
        elements_in_folder = [os.path.basename(e["path"]) for e in content['objects']]
    assert source_file_name not in elements_in_folder, "Failed to upload the file"
    clients().workspace.delete_content(destination_dir)


def test_run_code():
    code = '''
        a = 1
        b = 2
        c = a + b
        dbutils.notebook.exit(c)
    '''
    output = clients().run_python_code_on_notebook(code)
    assert output == "3", "Failed to compute code"


def test_cluster_delete():
    assert clients().cluster.delete_cluster_and_wait(), "Failed to delete cluster"
