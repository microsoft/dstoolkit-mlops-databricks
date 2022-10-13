az config set extension.use_dynamic_install=yes_without_prompt

echo "ClientID: $ARM_CLIENT_ID"
echo "Client Secret: $ARM_CLIENT_SECRET"
echo "Tenant ID: $ARM_TENANT_ID"

echo "Logging in using Azure service principal"
az login --service-principal -u $ARM_CLIENT_ID -p $ARM_CLIENT_SECRET --tenant $ARM_TENANT_ID

TOKEN_RESPONSE=$(az account get-access-token --resource $param_AZURE_DATABRICKS_APP_ID)
TOKEN=$(jq .accessToken -r <<< "$TOKEN_RESPONSE")

AZ_MGMT_RESOURCE_ENDPOINT=$(curl -X GET -H 'Content-Type: application/x-www-form-urlencoded' \
                            -d 'grant_type=client_credentials&client_id='$ARM_CLIENT_ID'&resource='$param_MANAGEMENT_RESOURCE_ENDPOINT'&client_secret='$ARM_CLIENT_SECRET \
                            https://login.microsoftonline.com/$ARM_TENANT_ID/oauth2/token)
MGMT_ACCESS_TOKEN=$(jq .access_token -r <<< "$AZ_MGMT_RESOURCE_ENDPOINT" )


### Save AAD Tokens As Environment Variables 

echo "Set Management Access Token As Environment Variable..."
echo "MGMT_ACCESS_TOKEN=$MGMT_ACCESS_TOKEN" >> $GITHUB_ENV

echo "Set AAD Token As Environment Variable..."
echo "TOKEN=$TOKEN" >> $GITHUB_ENV
