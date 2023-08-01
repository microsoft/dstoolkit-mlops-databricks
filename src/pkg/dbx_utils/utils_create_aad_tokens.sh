set -e

DATABRICKS_AAD_TOKEN=$( \
    az account get-access-token \
        --resource $DBX_RESOURCE_ID \
        --query "accessToken" \
        --output tsv \
)

DATABRICKS_MANAGEMENT_TOKEN=$( \
    az account get-access-token \
        --resource "https://management.core.windows.net/" \
        --query "accessToken" \
        --output tsv \
)

echo $DATABRICKS_AAD_TOKEN
echo $DATABRICKS_MANAGEMENT_TOKEN

echo "##vso[task.setvariable variable="DATABRICKS_MANAGEMENT_TOKEN";isOutput=true;]$DATABRICKS_MANAGEMENT_TOKEN"
echo "##vso[task.setvariable variable="DATABRICKS_AAD_TOKEN";isOutput=true;]$DATABRICKS_AAD_TOKEN" 