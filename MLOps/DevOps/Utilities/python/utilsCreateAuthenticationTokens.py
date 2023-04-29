import requests
import os


def create_management_token(token_request_body, token_request_headers, token_base_url):
    """
        Uses Our Service Principal Credentials To
        Generate Azure Active Directory Tokens
    """
    token_request_headers['resource'] = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'  #https://management.core.windows.net/  
    response = requests.get(token_base_url,
                            headers=token_request_headers,
                            data=token_request_body
                            )

    if response.status_code == 200:
        print(response.status_code)
    else:
        raise Exception(response.text)
    return response.json()['access_token']


def create_bearer_token(token_request_body, token_request_headers, token_base_url):
    """
        Uses Our Service Principal Credentials To
        Generate Azure Active Directory Tokens
    """
    token_request_body['resource'] = 'https://management.core.windows.net/' #2ff814a6-3304-4ab8-85cb-cd0e6f879c1d
    response = requests.get(token_base_url,
                            headers=token_request_headers,
                            data=token_request_body
                            )

    if response.status_code == 200:
        print(response.status_code)
    else:
        raise Exception(response.text)
    return response.json()['access_token']


if __name__ == "__main__":

    token_request_body = {
        'grant_type': 'client_credentials',
        'client_id': os.environ['ARM_CLIENT_ID'],
        'client_secret': os.environ['ARM_CLIENT_SECRET']
    }

    token_request_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    token_base_url = 'https://login.microsoftonline.com/' + \
        os.environ['ARM_TENANT_ID'] + \
        '/oauth2/token'

    bearer_token = create_bearer_token(
        token_request_body=token_request_body,
        token_request_headers=token_request_headers,
        token_base_url=token_base_url
        )

    management_token = create_management_token(
        token_request_body=token_request_body,
        token_request_headers=token_request_headers,
        token_base_url=token_base_url
        )

    os.environ['DATABRICKS_AAD_TOKEN'] = bearer_token
    os.environ['DATABRICKS_MANAGEMENT_TOKEN'] = management_token

    print("DATABRICKS_AAD_TOKEN", os.environ['DATABRICKS_AAD_TOKEN'])
    print("DATABRICKS_MANAGEMENT_TOKEN", os.environ['DATABRICKS_MANAGEMENT_TOKEN'])

    with open(os.getenv('GITHUB_ENV'), 'a') as env:
        print(f'DATABRICKS_AAD_TOKEN={bearer_token}', file=env)
        print(f'DATABRICKS_MANAGEMENT_TOKEN={management_token}', file=env)

    # print("##vso[task.setvariable variable=DATABRICKS_AAD_TOKEN;isOutput=true;]{b}".format(b=bearerToken))
    # print("##vso[task.setvariable variable=DATABRICKS_MANAGEMENT_TOKEN;isOutput=true;]{b}".format(b=managementToken))