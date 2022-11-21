

### Lets Retrieve Important Variables That Are Important For Later Steps

DATABRICKS_ORDGID=$(az databricks workspace list -g $param_parameters_resourceGroupName_value --query "[].workspaceId" -o tsv)
DATABRICKS_INSTANCE="$(az databricks workspace list -g $param_parameters_resourceGroupName_value --query "[].workspaceUrl" -o tsv)"
WORKSPACE_ID=$(az databricks workspace list -g $param_parameters_resourceGroupName_value --query "[].id" -o tsv)
AZ_KEYVAULT_NAME=$(az keyvault list -g $param_parameters_resourceGroupName_value --query "[].name" -o tsv)
#DATABRICKS_TOKEN=$(az keyvault secret show --name "dbkstoken" --vault-name $AZ_KEYVAULT_NAME --query "value" -o tsv)


# Creation Of Important Environment Variables For Later Steps.
echo "Set Environment Variables For Later Stages..."

echo "Set Key Vault Name As Environment Variable..."
echo "AZ_KEYVAULT_NAME=$AZ_KEYVAULT_NAME" >> $GITHUB_ENV

echo "Set Databricks OrgID As Environment Variable..."
echo "DATABRICKS_ORDGID=$DATABRICKS_ORDGID" >> $GITHUB_ENV

echo "Set Workspace ID As Environment Variable..."
echo "WORKSPACE_ID=$WORKSPACE_ID" >> $GITHUB_ENV

echo "Set Datbricks Instance As Environment Variable..."
echo "DATABRICKS_INSTANCE=$DATABRICKS_INSTANCE" >> $GITHUB_ENV

echo "Set Databricks Host As Environment Variable..."
echo "DATABRICKS_HOST=https://$DATABRICKS_INSTANCE" >> $GITHUB_ENV

echo "Set Databricks Token ID As Environment Variable..."
echo "DATABRICKS_TOKEN=$DATABRICKS_TOKEN" >> $GITHUB_ENV

echo "Set Python Path"
echo "PYTHONPATH=src/modules" >> $GITHUB_ENV