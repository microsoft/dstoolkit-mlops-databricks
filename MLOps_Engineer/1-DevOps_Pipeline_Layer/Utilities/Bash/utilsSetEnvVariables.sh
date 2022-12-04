
### Lets Retrieve Important Variables That Are Important For Later Steps

echo $Environment

echo "Ingest JSON File"
JSON=$( jq '.' MLOps_Engineer/2-Infrastructure_Layer/DBX_CICD_Deployment/Bicep_Params/$Environment/Bicep.parameters.json)
RESOURCE_GROUP_NAME=$( jq -r '.parameters.resourceGroupName.value' <<< "$JSON")
echo "Resource Group Name: $RESOURCE_GROUP_NAME"


DATABRICKS_ORDGID=$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].workspaceId" -o tsv)
DATABRICKS_INSTANCE="$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].workspaceUrl" -o tsv)"
WORKSPACE_ID=$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].id" -o tsv)
AZ_KEYVAULT_NAME=$(az keyvault list -g $RESOURCE_GROUP_NAME --query "[].name" -o tsv)
#DATABRICKS_TOKEN=$(az keyvault secret show --name "dbkstoken" --vault-name $AZ_KEYVAULT_NAME --query "value" -o tsv)


# Creation Of Important Environment Variables For Later Steps.
echo "Set Environment Variables For Later Stages..."

echo "Resource Group Name As Environment Variable..."
echo "##vso[task.setvariable variable="RESOURCE_GROUP_NAME";isOutput=true;]$RESOURCE_GROUP_NAME"

echo "Set Key Vault Name As Environment Variable..."
echo "##vso[task.setvariable variable="AZ_KEYVAULT_NAME";isOutput=true;]$AZ_KEYVAULT_NAME"

echo "Set Databricks OrgID As Environment Variable..."
echo "##vso[task.setvariable variable="DATABRICKS_ORDGID";isOutput=true;]$DATABRICKS_ORDGID"

echo "Set Workspace ID As Environment Variable..."
echo "##vso[task.setvariable variable="WORKSPACE_ID";isOutput=true;]$WORKSPACE_ID"

echo "Set Datbricks Instance As Environment Variable..."
echo "##vso[task.setvariable variable="DATABRICKS_INSTANCE";isOutput=true;]$DATABRICKS_INSTANCE"

echo "Set Databricks Host As Environment Variable..."
echo "##vso[task.setvariable variable="DATABRICKS_HOST";isOutput=true;]https://$DATABRICKS_INSTANCE"


# MORE RESEARCH -- Seems important to configure the python path for wheel file creation later on.
echo "Set Python Path"
echo "##vso[task.setvariable variable="PYTHONPATH";isOutput=true;]src/modules"
#echo "PYTHONPATH=src/modules" >> $GITHUB_ENV