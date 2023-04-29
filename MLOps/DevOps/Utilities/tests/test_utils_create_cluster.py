import os
import subprocess
import unittest
from unittest.mock import patch, MagicMock
from unittest import mock
import pytest
import json 
import requests

from python.utils_create_cluster import _list_clusters

class TestListClusters(unittest.TestCase):
    def _mock_response_successful(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"clusters": []}'
        return response
    
    def _mock_response_failure(self):
        response = requests.Response()
        response.status_code = 500
        response._content = b'{"clusters": []}'
        return response
    
    @patch('python.utils_create_cluster.WORKSPACE_ID', 'test_workspace_id')
    @patch('python.utils_create_cluster.DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
    @patch('python.utils_create_cluster.DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
    @patch('python.utils_create_cluster.DATABRICKS_INSTANCE', 'test_databricks_instance')
    @patch('requests.get')
    def test_list_clusters_successful(self, mock_get):
        mock_get.return_value = self._mock_response_successful()

        result, status_code = _list_clusters()

        dbkrs_req_headers = {
        'Authorization': 'Bearer test_databricks_aad_token',
        'X-Databricks-Azure-SP-Management-Token': 'test_databricks_management_token',
        'X-Databricks-Azure-Workspace-Resource-Id': 'test_workspace_id',
        'Content-Type': 'application/json'}

        mock_get.assert_called_once_with(
            'https://test_databricks_instance/api/2.0/clusters/list',
            headers=dbkrs_req_headers)
        
        #assert result == {"clusters": []}
        assert status_code == 200
    
    @patch('python.utils_create_cluster.WORKSPACE_ID', 'test_workspace_id')
    @patch('python.utils_create_cluster.DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
    @patch('python.utils_create_cluster.DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
    @patch('python.utils_create_cluster.DATABRICKS_INSTANCE', 'test_databricks_instance')
    @patch('requests.get')
    def test_list_clusters_failure(self, mock_get):
        mock_get.return_value = self._mock_response_failure()
        
        # If the response is not 200, then the main code "utils_crate_cluster.py" throws an error exception
        # The below is presumable capturing that error as a "good" things, as we expect this behaviour 
        with pytest.raises(Exception): 
            _list_clusters()
    

