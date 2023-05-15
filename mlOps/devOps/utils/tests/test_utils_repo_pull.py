import unittest
from unittest.mock import patch, MagicMock, mock_open
from unittest import mock
import pytest
from _pytest.monkeypatch import MonkeyPatch
import json 
import requests

from python.utils_repo_pull import _ingest_repo_param_file, get_repos_with_management_permissions, update_repo, main


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
    def test_get_repos_with_management_permissions_success(self, mock_get):
        monkeypatch = MonkeyPatch()

        monkeypatch.setenv('ARM_CLIENT_ID', 'test_arm_client_id')
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        mock_get.return_value.status_code = 200

        mock_return = {
            "repos":[
                {
                    "id":61449681029719,
                    "path":"/Repos/***/test_dbx_repo_folder_one",
                    "url":"https://github.com/test_repo_profile/test_repo_one",
                    "provider":"gitHub",
                    "branch":"main",
                    "head_commit_id":"test_commit_id"
                 }
            ]
        }

        mock_get.return_value.json.return_value = mock_return

        repos_with_management_permissions, status_code = get_repos_with_management_permissions()

        assert repos_with_management_permissions == mock_return["repos"]
        assert status_code == 200


        expected_dbkrs_req_headers = {
            'Authorization': 'Bearer test_databricks_aad_token',
            'X-Databricks-Azure-SP-Management-Token': 'test_databricks_management_token',
            'X-Databricks-Azure-Workspace-Resource-Id': 'test_workspace_id',
            'Content-Type': 'application/json'}
        

        mock_get.assert_called_once_with(
            'https://test_databricks_instance/api/2.0/repos',
            headers=expected_dbkrs_req_headers
        )

        
        @patch('requests.get')
        def test_get_repos_with_management_permissions_failure(mock_get):
            monkeypatch = MonkeyPatch()

            monkeypatch.setenv('ARM_CLIENT_ID', 'test_arm_client_id')
            monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
            monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
            monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
            monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

            mock_get.return_value.status_code = 500

            with pytest.raises(Exception) as e:
                status_code = get_repos_with_management_permissions()
                assert status_code == 500


# update_repo

class UpdateRepo(unittest.TestCase):
    
    @patch('requests.patch')
    def test_update_repo_success(self, mock_patch):
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('ARM_CLIENT_ID', 'test_arm_client_id')
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        mock_repo_id = 123456789
        mock_update_branch = "test_main_branch"

        mock_patch.return_value.status_code = 200

        status_code = update_repo(mock_repo_id, mock_update_branch )

        assert status_code == 200


        expected_dbkrs_req_headers = {
            'Authorization': 'Bearer test_databricks_aad_token',
            'X-Databricks-Azure-SP-Management-Token': 'test_databricks_management_token',
            'X-Databricks-Azure-Workspace-Resource-Id': 'test_workspace_id',
            'Content-Type': 'application/json'}
        
        mock_patch.assert_called_once_with(
            "https://test_databricks_instance/api/2.0/repos/" + str(mock_repo_id),
            headers=expected_dbkrs_req_headers,
            json={
                "branch": mock_update_branch
            }
        )

    @patch('requests.post')
    def test_update_repo_failure(self, mock_post):

        mock_repo_id = 123456789
        mock_update_branch = "test_main_branch"
        
        mock_post.return_value.status_code = 500

        with pytest.raises(Exception) as e:
            status_code = update_repo(mock_repo_id, mock_update_branch )
            assert status_code == 500


class Main(unittest.TestCase):

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

 
    @patch('python.utils_repo_pull.update_repo')
    @patch('python.utils_repo_pull.get_repos_with_management_permissions')
    @patch('python.utils_repo_pull._ingest_repo_param_file')
    def test_main_success(self, mock_ingest_repo_param_file, mock_get_repos_with_management_permissions, mock_update_repo):
        
        # monkey patch environment variables
        monkeypatch = MonkeyPatch()
        monkeypatch.setenv('ENVIRONMENT', 'test_environment')
        monkeypatch.setenv('ARM_CLIENT_ID', 'test_arm_client_id')
        monkeypatch.setenv('WORKSPACE_ID', 'test_workspace_id')
        monkeypatch.setenv('DATABRICKS_MANAGEMENT_TOKEN', 'test_databricks_management_token')
        monkeypatch.setenv('DATABRICKS_AAD_TOKEN', 'test_databricks_aad_token')
        monkeypatch.setenv('DATABRICKS_INSTANCE', 'test_databricks_instance')

        
        mock_ingest_repo_param_file_json_return = [{
                "url":  "test_url",
                "provider":  "test_provider",
                "path":  "test_folder",
                "branch": "test_branch"
            }]
        
        mock_ingest_repo_param_file.return_value = mock_ingest_repo_param_file_json_return

        # Should be doing a mock open instead !!!
        #mock_ingest_repo_param_file.return_value = mock_ingest_repo_param_file_json_return

        # mock return value from get repos with management permissions
        mock_get_repos_with_management_permissions_json_return = [
            {
                "id":61449681029719,
                "path":"/Repos/***/test_folder",
                "url":"https://github.com/test_repo_profile/test_repo_one",
                "provider":"gitHub",
                "branch":"main",
                "head_commit_id":"test_commit_id"
            }
        ]

        mock_get_repos_with_management_permissions.return_value = (mock_get_repos_with_management_permissions_json_return, 200)

        # mock return value from update repo
        mock_update_repo.return_value = 200

        # call main function
        status_code = main()

        # assert main function returns 200
        assert status_code == 200

        
        # assert mock functions were called using correct arguments 
        mock_ingest_repo_param_file.assert_called_once_with('mlOps/devOps/params/test_environment/repos.json')
        mock_update_repo.assert_called_once_with("61449681029719", "test_branch")

        








        





