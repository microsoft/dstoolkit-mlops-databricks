
### Lets Retrieve Important Variables That Are Important For Later Steps

echo $ENVIRONMENT

echo "Ingest JSON File"
JSON=$( jq '.' .github/MLOps_Engineer/Infrastructure/DBX_CICD_Deployment/Bicep_Params/$ENVIRONMENT/Bicep.parameters.json)


RESOURCE_GROUP_NAME=$( jq -r '.parameters.resourceGroupName.value' <<< "$JSON")
echo "Resource Group Name: $RESOURCE_GROUP_NAME"


DATABRICKS_ORDGID=$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].workspaceId" -o tsv)
DATABRICKS_INSTANCE="$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].workspaceUrl" -o tsv)"
WORKSPACE_ID=$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].id" -o tsv)
AZ_KEYVAULT_NAME=$(az keyvault list -g $RESOURCE_GROUP_NAME --query "[].name" -o tsv)
SUBSCRIPTION_ID=$( az account show --query id -o tsv )
echo $SUBSCRIPTION_ID
echo $DATABRICKS_ORDGID
echo $WORKSPACE_ID
echo $AZ_KEYVAULT_NAME
echo $SUBSCRIPTION_ID
#DATABRICKS_TOKEN=$(az keyvault secret show --name "dbkstoken" --vault-name $AZ_KEYVAULT_NAME --query "value" -o tsv)


# Creation Of Important Environment Variables For Later Steps.
echo "Set Environment Variables For Later Stages..."

echo "Set Environment Name As Environment Variable..."
echo "ENVIRONMENT=$ENVIRONMENT" >> $GITHUB_ENV

echo "Set Resource Group Name Name As Environment Variable..."
echo "RESOURCE_GROUP_NAME=$RESOURCE_GROUP_NAME" >> $GITHUB_ENV

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

echo "Set SUBSCRIPTION_ID As Environment Variable..."
echo "SUBSCRIPTION_ID=$SUBSCRIPTION_ID" >> $GITHUB_ENV

#echo "Set Python Path"
#echo "PYTHONPATH=src/modules" >> $GITHUB_ENV