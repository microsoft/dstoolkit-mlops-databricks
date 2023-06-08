
### Lets Retrieve Important Variables That Are Important For Later Steps

echo $ENVIRONMENT

echo "Ingest JSON File"
JSON=$( jq '.' infrastructure/bicep/params/$ENVIRONMENT/bicep.parameters.json)


RESOURCE_GROUP_NAME=$( jq -r '.parameters.resourceGroupName.value' <<< "$JSON")
echo "Resource Group Name: $RESOURCE_GROUP_NAME"

DATABRICKS_WS_NAME=$( az databricks workspace list -g $RESOURCE_GROUP_NAME --query [].name -o tsv )
AML_WS_NAME=$(az ml workspace list -g $RESOURCE_GROUP_NAME  --query [].workspaceName -o tsv)
DATABRICKS_ORDGID=$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].workspaceId" -o tsv)
DATABRICKS_INSTANCE="$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].workspaceUrl" -o tsv)"
WORKSPACE_ID=$(az databricks workspace list -g $RESOURCE_GROUP_NAME --query "[].id" -o tsv)
AZ_KEYVAULT_NAME=$(az keyvault list -g $RESOURCE_GROUP_NAME --query "[].name" -o tsv)
SUBSCRIPTION_ID=$( az account show --query id -o tsv )
#echo $SUBSCRIPTION_ID
#echo $DATABRICKS_ORDGID
#echo $WORKSPACE_ID
#echo $AZ_KEYVAULT_NAME
#echo $SUBSCRIPTION_ID
#echo $AML_WS_NAME
#echo $DATABRICKS_WS_NAME
#DATABRICKS_TOKEN=$(az keyvault secret show --name "dbkstoken" --vault-name $AZ_KEYVAULT_NAME --query "value" -o tsv)


if [[ $DevOps_Agent == "GitHub" ]]; then
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

    #echo "Set Databricks Token ID As Environment Variable..."
    #echo "DATABRICKS_TOKEN=$DATABRICKS_TOKEN" >> $GITHUB_ENV

    echo "Set SUBSCRIPTION_ID As Environment Variable..."
    echo "SUBSCRIPTION_ID=$SUBSCRIPTION_ID" >> $GITHUB_ENV

    echo "Set AML_WS_NAME As Environment Variable..."
    echo "AML_WS_NAME=$AML_WS_NAME" >> $GITHUB_ENV

    echo "Set DATABRICKS_WS_NAME As Environment Variable..."
    echo "DATABRICKS_WS_NAME=$DATABRICKS_WS_NAME" >> $GITHUB_ENV
    
else

    # Creation Of Important Environment Variables For Later Steps.
    echo "Set Environment Variables For Later Stages..."


    echo "ENVIRONMENT Name As Environment Variable..."
    echo "##vso[task.setvariable variable="ENVIRONMENT";isOutput=true;]$ENVIRONMENT"


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

    echo "Set Databricks Host As Environment Variable..."
    echo "##vso[task.setvariable variable="SUBSCRIPTION_ID";isOutput=true;]$SUBSCRIPTION_ID"

    echo "Set AML_WS_NAME As Environment Variable..."
    echo "##vso[task.setvariable variable="AML_WS_NAME";isOutput=true;]$AML_WS_NAME"

    echo "Set DATABRICKS_WS_NAME As Environment Variable..."
    echo "##vso[task.setvariable variable="DATABRICKS_WS_NAME";isOutput=true;]$DATABRICKS_WS_NAME"
fi
