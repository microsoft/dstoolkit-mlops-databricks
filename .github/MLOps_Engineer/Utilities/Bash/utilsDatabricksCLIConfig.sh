#!/usr/bin/env bash
pip3 install databricks-cli --upgrade


echo "Ingest JSON File"
JSON=$( jq '.' MLOps_Engineer/2-Infrastructure_Layer/DBX_CICD_Deployment/Bicep_Params/$Environment/Bicep.parameters.json)
resourceGroupName=$( jq -r '.parameters.resourceGroupName.value' <<< "$JSON")
echo "Resource Group Name: $resourceGroupName"

AZ_KEYVAULT_NAME=$(az keyvault list -g $resourceGroupName --query "[].name" -o tsv)
DATABRICKS_TOKEN=$(az keyvault secret show --name "dbkstoken" --vault-name $AZ_KEYVAULT_NAME --query "value" -o tsv)

echo "Set Databricks Token As Environment Variable..."
echo "##vso[task.setvariable variable="DATABRICKS_TOKEN";isOutput=true;]$DATABRICKS_TOKEN"

echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN


# Change absolutely NOTHING.
# DATABRICKS_HOST : It Must Start As https:// : It Must Not End In '/'
# DATABRICKS_TOKEN : It Must Not Be Expired. 


databricks configure --token <<EOF
$DATABRICKS_HOST
$DATABRICKS_TOKEN
EOF

# Different behaviour between Github Actions Bash and ADO AzCLI. The former authenticates with databricks configure --token only.
#databricks configure --token 

echo "Test Databricks CLI Commands"
databricks -h 
databricks fs ls

#databricks fs mkdirs dbfs:/tmp/new-dir