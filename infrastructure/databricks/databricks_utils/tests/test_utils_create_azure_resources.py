import os
import json
import subprocess
import unittest
import pytest
from unittest.mock import patch, mock_open
from python.utils_create_azure_resources import deploy_azure_resources, run_cmd, LoadJson



test_json = {"parameters": {
            "TemplateFilePath": {
                "value": "mlOps/devOps/infra/master_templates/main.bicep"
            },
            "TemplateParamFilePath": {
                "value": "mlOps/devOps/infra/master_templates/params/development/bicep.parameters.json"
            },
            "location": {
                "value": "eastus"
            }
        }
    }

class TestLoadJson:
    @patch("builtins.open", new_callable=mock_open, read_data=test_json)
    def test_load_json(self, mock_file):
        load_json = LoadJson()
        result = load_json.load_json()
        expected_result = {
            "parameters": {
                "TemplateFilePath": {
                    "value": "mlOps/devOps/infra/master_templates/main.bicep"
                },
                "TemplateParamFilePath": {
                    "value": "mlOps/devOps/infra/master_templates/params/development/bicep.parameters.json"
                },
                "location": {
                    "value": "eastus"
                }
            }
        }
        assert result == expected_result


    @patch.object(LoadJson, "load_json", return_value=test_json)
    def test_get_param_file_path(self, mock_load_json):
        load_json = LoadJson()
        result = load_json.get_param_file_path()
        expected_result = "mlOps/devOps/infra/master_templates/params/development/bicep.parameters.json"
        assert result == expected_result


    @patch.object(LoadJson, "load_json", return_value=test_json)
    def test_get_template_file_path(self, mock_load_json):
        load_json = LoadJson()
        result = load_json.get_template_file_path()
        expected_result = "mlOps/devOps/infra/master_templates/main.bicep"
        assert result == expected_result


    @patch.object(LoadJson, "load_json", return_value=test_json)
    def test_get_location(self, mock_load_json):
        load_json = LoadJson()
        result = load_json.get_location()
        expected_result = "eastus"
        assert result == expected_result


class TestRunCmd:
    def test_run_cmd_success(self):
        cmd = ["echo", "Hello, world!"]
        result = run_cmd(cmd)
        expected_result = ["Hello, world!"]
        assert result == expected_result

    def test_run_cmd_error(self):
        cmd = ["nonexistentcommand"]
        with pytest.raises(RuntimeError):
            run_cmd(cmd)


class TestDeployAzureResources:
    @patch("python.utils_create_azure_resources.run_cmd")
    @patch.object(LoadJson, "get_param_file_path", return_value="mlOps/devOps/infra/master_templates/params/development/bicep.parameters.json")
    @patch.object(LoadJson, "get_template_file_path", return_value="mlOps/devOps/infra/master_templates/main.bicep")
    @patch.object(LoadJson, "get_location", return_value="eastus")
    def test_deploy_azure_resources_success(self, mock_location, mock_template_file_path, mock_param_file_path, mock_run_cmd):
        mock_run_cmd.return_value = ('', 0)
        deploy_azure_resources()
        mock_run_cmd.assert_called_with(
            [
                "az", "deployment", "sub", "create",
                "--location", "eastus",
                "--template-file", "path/to/template/file",
                "--parameters", "path/to/param/file",
                "--name", "test_environment",
            ]
        )



























def test_load_json():
    # Mock the `open` function to return a mocked file object
    mocked_open = mock_open(read_data='''{
                                "parameters": {
                                    "TemplateFilePath": {
                                        "value": "mlOps/devOps/infra/master_templates/main.bicep"
                                    },
                                    "TemplateParamFilePath": {
                                        "value": "mlOps/devOps/infra/master_templates/params/development/bicep.parameters.json"
                                    },
                                    "location": {
                                        "value": "eastus"
                                    }
                                }
                            }''')

    with patch('builtins.open', mocked_open):
        # Create an instance of the LoadJson class
        load_json_obj = LoadJson()

        # Call the load_json method and assert that it returns the expected dictionary
        assert load_json_obj.load_json() == {
            'parameters': {
                'TemplateFilePath': {
                    'value': 'mlOps/devOps/infra/master_templates/main.bicep'
                },
                'TemplateParamFilePath': {
                    'value': 'mlOps/devOps/infra/master_templates/params/development/bicep.parameters.json'
                },
                'location': {
                    'value': 'eastus'
                }
            }
        }

        # Call the get_template_file_path method and assert that it returns the expected value
        assert load_json_obj.get_template_file_path() == 'mlOps/devOps/infra/master_templates/main.bicep'

        # Call the get_param_file_path method and assert that it returns the expected value
        assert load_json_obj.get_param_file_path() == 'mlOps/devOps/infra/master_templates/params/development/bicep.parameters.json'

        # Call the get_location method and assert that it returns the expected value
        assert load_json_obj.get_location() == 'eastus'


class TestRunCmd(unittest.TestCase):
    def test_run_cmd(self):
        test_cmd = ['echo', 'hello, world']
        output, return_code = run_cmd(test_cmd)
        self.assertEqual(return_code, 0)
        self.assertEqual(output, ['hello, world'])

    def test_run_cmd_failure(self):
        test_cmd = ['12345']
        with self.assertRaises(subprocess.CalledProcessError):
            run_cmd(test_cmd)


class TestCreateAzureResources(unittest.TestCase):
    
    @patch('python.utils_azure_login.ARM_TENANT_ID', 'test_tenant_id')
    @patch('python.utils_azure_login.ARM_CLIENT_SECRET', 'test_client_secret')
    @patch('python.utils_azure_login.ARM_CLIENT_ID', 'test_client_id')
    @patch('python.utils_azure_login.run_cmd')
    def test_start_azure_login(self, mock_run_cmd):
        mock_run_cmd.return_value = ('', 0)
        return_code = deploy_azure_resources()
        self.assertEqual(return_code, 0) # 0 is the expected return code for a successful login

        # This must be assessing the correct parameters are being passed to the run_cmd function
        # If someone changes the code, then the tests will fail. 
        
        mock_run_cmd.assert_called_once_with(
            [
                "az", "deployment", "sub", "create",
                "--location", location,
                "--template-file", template_file_path,
                "--parameters", template_param_file_path,
                "--name", ENVIRONMENT,
                "--only-show-errors"  ]
        )
        
