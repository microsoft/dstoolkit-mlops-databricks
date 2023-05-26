import os
import subprocess
import unittest
from unittest.mock import patch

from python.utils_azure_login import start_azure_login, run_cmd


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


class TestAzureLogin(unittest.TestCase):
    
    @patch('python.utils_azure_login.ARM_TENANT_ID', 'test_tenant_id')
    @patch('python.utils_azure_login.ARM_CLIENT_SECRET', 'test_client_secret')
    @patch('python.utils_azure_login.ARM_CLIENT_ID', 'test_client_id')
    @patch('python.utils_azure_login.run_cmd')
    def test_start_azure_login(self, mock_run_cmd):
        mock_run_cmd.return_value = ('', 0)
        return_code = start_azure_login()
        self.assertEqual(return_code, 0) # 0 is the expected return code for a successful login

        # This must be assessing the correct parameters are being passed to the run_cmd function
        # If someone changes the code, then the tests will fail
        mock_run_cmd.assert_called_once_with(
            [
                'az', 'login',
                '--service-principal',
                '-u', 'test_client_id',
                '-p', 'test_client_secret',
                '--tenant', 'test_tenant_id'
            ]
        )
