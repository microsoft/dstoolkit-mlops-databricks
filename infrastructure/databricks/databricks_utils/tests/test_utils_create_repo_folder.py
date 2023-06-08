import unittest
from unittest.mock import patch, MagicMock, mock_open
from unittest import mock
import pytest
from _pytest.monkeypatch import MonkeyPatch
import json 
import requests

from python.utils_create_repo_folder import _ingest_repo_param_file, create_databricks_repos


class TestCreateRepoFolder(unittest.TestCase):

    @patch('requests.post')
    def test_create_databricks_repos_success(self, mock_post):
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('ARM_CLIENT_ID', 'test_arm_client_id')
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')
        

        mock_post.return_value.status_code = 200

        mock_repo_json = {
                "url":  "test_url",
                "provider":  "test_provider",
                "path":  "test_folder"
            }
        
        status_code = create_databricks_repos(mock_repo_json)

        assert status_code == 200
        expected_dbkrs_req_headers = {
            'Authorization': 'Bearer test_databricks_aad_token',
            'X-Databricks-Azure-SP-Management-Token': 'test_databricks_management_token',
            'X-Databricks-Azure-Workspace-Resource-Id': 'test_workspace_id',
            'Content-Type': 'application/json'}
        
        mock_post.assert_called_once_with(
            'https://test_databricks_instance/api/2.0/repos',
            headers=expected_dbkrs_req_headers,
            json=mock_repo_json)

    @patch('requests.post')
    def test_create_databricks_repos_failure(self, mock_post):
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('ARM_CLIENT_ID', 'test_arm_client_id')
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        mock_post.return_value.status_code = 400

        mock_repo_json = {
                "url":  "test_url",
                "provider":  "test_provider",
                "path":  "test_folder"
            }

        with pytest.raises(Exception) as e:
            status_code = create_databricks_repos(mock_repo_json)
            assert status_code == 400
    


class TestIngestRepoParamFile(unittest.TestCase):

    test_repo_json = {
        "Git_Configuration": [ 
            {
            "git_username":  "test_username",
            "git_provider":  "test_provider",
            }
        ],
        "Repo_Configuration": [
            {
                "url":  "test_url",
                "provider":  "test_provider",
                "path":  "test_folder"
            }
        ]
    }

    test_repo_json = json.dumps(test_repo_json)


    @patch("builtins.open", new_callable=mock_open, read_data=test_repo_json)
    def test_load_json(self, mock_open):
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('ENVIRONMENT', 'test_environment')
        #cluster = Cluster()

        result = _ingest_repo_param_file( "test_cluster_param_file.json")
        
        # Expected result is an array and not an object
        expected_result = [
            {
                "url":  "test_url",
                "provider":  "test_provider",
                "path":  "test_folder"
            }
        ]
        assert result == expected_result