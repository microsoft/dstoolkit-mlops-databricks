import os
from typing import Dict

def get_databricks_request_headers() -> Dict[str, str]:
    """
    Gets the Databricks headers required for API calls using
    the Databricks AAD token, Databricks Management token and
    Databricks Workspace ID from the environment variables.

    :return: databricks_req_headers
    :type: dict
    """
    workspace_id = os.environ.get("WORKSPACE_ID")
    databricks_aad_token = os.environ.get("DATABRICKS_AAD_TOKEN")
    databricks_mgmt_token = os.environ.get("DATABRICKS_MANAGEMENT_TOKEN")

    databricks_req_headers = {
        'Authorization': f'Bearer {databricks_aad_token}',
        'X-Databricks-Azure-SP-Management-Token': f'{databricks_mgmt_token}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'{workspace_id}',
        'Content-Type': 'application/json'
    }
    return databricks_req_headers