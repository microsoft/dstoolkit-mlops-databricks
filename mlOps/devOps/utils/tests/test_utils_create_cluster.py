import unittest
from unittest.mock import patch, MagicMock, mock_open
from unittest import mock
import pytest
from _pytest.monkeypatch import MonkeyPatch
import json 
import requests



from python.utils_create_cluster import _list_existing_clusters, _get_cluster_names, _ingest_cluster_param_file, _create_cluster, _manage_cluster_state, _get_databricks_cluster_info # _list_clusters


class TestListExistingClusters(unittest.TestCase):

    @patch("requests.get")
    def test_list_existing_clusters_successfull(self, mock_get):
        # Setup
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        # Mock Api Response
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "clusters": [
                {
                    "cluster_name": "test_cluster_name",
                    "cluster_id": "test_cluster_id"
                }
            ]
        }

        existing_clusters, status_code = _list_existing_clusters()
        
        # Test Assertions
        assert existing_clusters["clusters"][0]["cluster_name"] == "test_cluster_name"
        assert existing_clusters["clusters"][0]["cluster_id"] == "test_cluster_id"
        assert status_code == 200
        
        dbkrs_req_headers = {
            'Authorization': 'Bearer test_databricks_aad_token',
            'X-Databricks-Azure-SP-Management-Token': 'test_databricks_management_token',
            'X-Databricks-Azure-Workspace-Resource-Id': 'test_workspace_id',
            'Content-Type': 'application/json'}
        mock_get.assert_called_once_with(
            'https://test_databricks_instance/api/2.0/clusters/list',
            headers=dbkrs_req_headers)
        

    @patch("requests.get")
    def test_list_existing_clusters_failure(self, mock_get):
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        mock_get.return_value.status_code = 400
        
        with pytest.raises(Exception) as e:
            _, status_code = _list_existing_clusters()

               
class TestGetClusterNames(unittest.TestCase):
    def test_get_cluster_names_successful(self):
        mock_existing_clusters = {
            "clusters": [
                {
                    "cluster_name": "test-cluster-1",
                    "cluster_id": "cluster-1-id"
                },
                {
                    "cluster_name": "test-cluster-2",
                    "cluster_id": "cluster-2-id"
                }
            ]
        }
        result = _get_cluster_names(mock_existing_clusters)

        assert result == ["test-cluster-1", "test-cluster-2"]

    def test_get_cluster_name_failure(self):
        mock_existing_clusters = {
            "clusters": [
                {
                    "cluster_name": "test-cluster-1",
                    "cluster_id": "cluster-1-id"
                },
                {
                    "cluster_name": "test-cluster-2",
                    "cluster_id": "cluster-2-id"
                }
            ]
        }
        result = _get_cluster_names(mock_existing_clusters)

        assert result != ["test-cluster-1", "test-cluster-3"]



class TestIngestClusterParamFile(unittest.TestCase):

    test_cluster_json = {
        "Clusters":  [
            {
                "cluster_name":  "ml_cluster",
                "spark_version":  "11.3.x-cpu-ml-scala2.12",
                "node_type_id":  "Standard_DS3_v2",
                "spark_conf":  {
                            },
                "autotermination_minutes":  30,
                "runtime_engine":  "STANDARD",
                "autoscale":  {
                                "min_workers":  2,
                                "max_workers":  3
                            }
            }
        ]
    }

    test_cluster_json = json.dumps(test_cluster_json)


    @patch("builtins.open", new_callable=mock_open, read_data=test_cluster_json)
    def test_load_json(self, mock_open):
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('ENVIRONMENT', 'test_environment')
        #cluster = Cluster()

        result = _ingest_cluster_param_file( "test_cluster_param_file.json")
        
        # Expected result is an array and not an object
        expected_result = [
                {
                    "cluster_name":  "ml_cluster",
                    "spark_version":  "11.3.x-cpu-ml-scala2.12",
                    "node_type_id":  "Standard_DS3_v2",
                    "spark_conf":  {
                                },
                    "autotermination_minutes":  30,
                    "runtime_engine":  "STANDARD",
                    "autoscale":  {
                                    "min_workers":  2,
                                    "max_workers":  3
                                }
                }
            ]
        assert result == expected_result


class TestCreateCluster(unittest.TestCase):
    
    @patch('requests.post')
    def test_create_cluster_successful(self, mock_post):
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        mock_post.return_value.status_code = 200
        
        mock_json_cluster = [
            {
                "cluster_name":  "ml_cluster",
                "spark_version":  "11.3.x-cpu-ml-scala2.12",
                "node_type_id":  "Standard_DS3_v2",
                "spark_conf":  {
                            },
                "autotermination_minutes":  30,
                "runtime_engine":  "STANDARD",
                "autoscale":  {
                                "min_workers":  2,
                                "max_workers":  3
                            }
            }
        ]
        status_code, _ = _create_cluster(mock_json_cluster)

        assert status_code == 200

        expected_dbkrs_req_headers = {
            'Authorization': 'Bearer test_databricks_aad_token',
            'X-Databricks-Azure-SP-Management-Token': 'test_databricks_management_token',
            'X-Databricks-Azure-Workspace-Resource-Id': 'test_workspace_id',
            'Content-Type': 'application/json'}
        
        mock_post.assert_called_once_with(
            'https://test_databricks_instance/api/2.0/clusters/create',
            headers=expected_dbkrs_req_headers,
            json=mock_json_cluster) 


    @patch('requests.post')
    def test_create_cluster_failure(self, mock_post):
        mock_post.return_value.status_code = 500
        
        mock_json_cluster = [
            {
                "cluster_name":  "ml_cluster",
                "spark_version":  "11.3.x-cpu-ml-scala2.12",
                "node_type_id":  "Standard_DS3_v2",
                "spark_conf":  {
                            },
                "autotermination_minutes":  30,
                "runtime_engine":  "STANDARD",
                "autoscale":  {
                                "min_workers":  2,
                                "max_workers":  3
                            }
            }
        ]

        with pytest.raises(Exception) as e:
            _, status_code = _create_cluster(mock_json_cluster)
            #assert status_code == 500



        
class TestGetDatabricksClusterInfo(unittest.TestCase):
    
    @mock.patch('requests.get')
    def test_get_databricks_cluster_info_success(self, mock_get):
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        mock_get.return_value.status_code = 200
        # set up mock response

        
        expected_response = {"state": "RUNNING"}
        
        mock_get.return_value.json.return_value = expected_response
  

        cluster_id = 'fake_cluster_id'
        actual_response_content = _get_databricks_cluster_info(cluster_id)

        assert actual_response_content == expected_response


        DBRKS_CLUSTER_ID = {'cluster_id': cluster_id}
        expected_dbkrs_req_headers = {
            'Authorization': 'Bearer test_databricks_aad_token',
            'X-Databricks-Azure-SP-Management-Token': 'test_databricks_management_token',
            'X-Databricks-Azure-Workspace-Resource-Id': 'test_workspace_id',
            'Content-Type': 'application/json'}
        
        mock_get.assert_called_once_with(
            'https://test_databricks_instance/api/2.0/clusters/get',
            headers=expected_dbkrs_req_headers, params=DBRKS_CLUSTER_ID) 
    


        
#class TestManageClusterState(unittest.TestCase):

#    @mock.patch('python.utils_create_cluster._get_databricks_cluster_info')
#    def test__manage_cluster_state_failure(self, mock_get_databricks_cluster_info):
#        
#        cluster_id = 'fake_cluster_id'
        
        # cluster in TERMINATED state
#        mock_get_databricks_cluster_info.return_value = {'state': 'TERMINATED'}
#        with pytest.raises(ValueError):
#            _manage_cluster_state(cluster_id)
    
#    @patch('time.sleep')
#    @mock.patch('python.utils_create_cluster._get_databricks_cluster_info')
#    def test__manage_cluster_state_restarting(self, mock_get_databricks_cluster_info, mock_sleep):

        
#        cluster_id = 'fake_cluster_id'
#        mock_sleep.return_value = None
#        # cluster in RESTARTING state
#        mock_get_databricks_cluster_info.return_value = {'state': 'RESTARTING'}
#        #_manage_cluster_state(cluster_id)

#        assert _manage_cluster_state(cluster_id)['state'] == 'RESTARTING'
#        mock_get_databricks_cluster_info.assert_called_with(cluster_id)
#        #mock_get_databricks_cluster_info.reset_mock()
        

        # cluster in PENDING state
        #mock_get_databricks_cluster_info.return_value = {'state': 'PENDING'}
        #_manage_cluster_state(cluster_id)

        #assert _manage_cluster_state(cluster_id)['state'] == 'PENDING'
        #mock_get_databricks_cluster_info.assert_called_with(cluster_id)
        #mock_get_databricks_cluster_info.reset_mock()
        
        # cluster in RUNNING state
        #mock_get_databricks_cluster_info.return_value = {'state': 'RUNNING'}
        #_manage_cluster_state(cluster_id)

        #assert _manage_cluster_state(cluster_id)['state'] == 'RUNNING'
        #mock_get_databricks_cluster_info.assert_called_with(cluster_id)