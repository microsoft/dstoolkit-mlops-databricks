import os
import argparse
import pandas as pd
from sklearn.model_selection import train_test_split
import logging
import mlflow
import requests
import os

#parser.add_argument("--test_train_ratio", type=float, required=False, default=0.25)

def main():
    """Main function of the script."""

    # input and output arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("--data", type=str, help="path to input data")
    parser.add_argument("--test_train_ratio", type=float, required=False, default=0.25)
    parser.add_argument("--train_data", type=str, help="path to train data")
    parser.add_argument("--test_data", type=str, help="path to test data")

    args = parser.parse_args()
    # Start Logging
    mlflow.start_run()

    print(" ".join(f"{k}={v}" for k, v in vars(args).items()))

    print("input data:", args.data)

    credit_df = pd.read_excel(args.data, header=1, index_col=0)

    mlflow.log_metric("num_samples", credit_df.shape[0])
    mlflow.log_metric("num_features", credit_df.shape[1] - 1)

    credit_train_df, credit_test_df = train_test_split(
        credit_df,
        test_size=args.test_train_ratio,
    )

    # output paths are mounted as folder, therefore, we are adding a filename to the path
    credit_train_df.to_csv(os.path.join(args.train_data, "data.csv"), index=False)

    credit_test_df.to_csv(os.path.join(args.test_data, "data.csv"), index=False)

    # Stop Logging
    mlflow.end_run()



    # Retrieve Tokens 


def createManagementToken(tokenRequestBody, tokenRequestHeaders, tokenBaseURL):
        """
            Uses Our Service Principal Credentials To Generate Azure Active Directory Tokens
        """

        tokenRequestBody['resource'] = 'https://management.core.windows.net/'
        
        response = requests.get(tokenBaseURL, headers=tokenRequestHeaders, data=tokenRequestBody)
        
        if response.status_code == 200:
            print(response.status_code)
        
        else:
            raise Exception(response.text)
        
        return response.json()['access_token']

def createBearerToken(tokenRequestBody, tokenRequestHeaders, tokenBaseURL):
        """
            Uses Our Service Principal Credentials To Generate Azure Active Directory Tokens
        """
        
        tokenRequestBody['resource'] = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'
        
        response = requests.get(tokenBaseURL, headers=tokenRequestHeaders, data=tokenRequestBody)
        
        if response.status_code == 200:
            print(response.status_code)
        
        else:
            raise Exception(response.text)
        
        return response.json()['access_token']



def listClusters(DBRKS_REQ_HEADERS, DATABRICKS_INSTANCE):
    """
        Returns a Json object containing a list of existing Databricks Clusters.
    """

    response = requests.get('https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/list', headers=DBRKS_REQ_HEADERS)

    if response.status_code != 200:
        raise Exception(response.content)

    else:
        return response.json()



if __name__ == "__main__":

    # The sp credentials need to come in from key vault 

    tokenRequestBody = {
        'grant_type': 'client_credentials',
        'client_id': ' ',
        'client_secret': ' '
    } 
    tokenRequestHeaders = {'Content-Type': 'application/x-www-form-urlencoded'}
    tokenBaseURL = 'https://login.microsoftonline.com/' + ' ' + '/oauth2/token'

    DBRKS_BEARER_TOKEN = createBearerToken(tokenRequestBody=tokenRequestBody, 
                                    tokenRequestHeaders=tokenRequestHeaders, 
                                    tokenBaseURL=tokenBaseURL
                    )
    
    DBRKS_MANAGEMENT_TOKEN = createManagementToken(tokenRequestBody=tokenRequestBody,
                                            tokenRequestHeaders=tokenRequestHeaders,
                                            tokenBaseURL=tokenBaseURL
                    )


    DBRKS_REQ_HEADERS = {
    'Authorization': f'Bearer {DBRKS_BEARER_TOKEN}',
    'X-Databricks-Azure-SP-Management-Token': f'{DBRKS_MANAGEMENT_TOKEN}',
    'X-Databricks-Azure-Workspace-Resource-Id': '/subscriptions/<>/resourceGroups/databricks-sandbox-rg/providers/Microsoft.Databricks/workspaces/dbxwssandbox-eco3',
    'Content-Type': 'application/json'
}
    DATABRICKS_INSTANCE = "adb-204110209##.#.azuredatabricks.net"

    existingClusters = listClusters(DBRKS_REQ_HEADERS, DATABRICKS_INSTANCE)

    print(existingClusters)
