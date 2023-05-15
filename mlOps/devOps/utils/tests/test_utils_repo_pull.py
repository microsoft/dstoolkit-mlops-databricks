import unittest
from unittest.mock import patch, MagicMock, mock_open
from unittest import mock
import pytest
from _pytest.monkeypatch import MonkeyPatch
import json 
import requests

from python.utils_repo_pull import _ingest_repo_param_file, get_repos_with_management_permissions, update_repo


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

#get_repos_with_management_permissions
class GetReposWithManagementPermissions(unittest.TestCase):

    @patch('requests.get')
    def test_get_repos_with_management_permissions_success(mock_get):
        monkeypatch = MonkeyPatch()

        monkeypatch.setenv('ARM_CLIENT_ID', 'test_arm_client_id')
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        mock_get.return_value.status_code = 200

        




        pass