#!/usr/bin/env bash

echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
pip3 install databricks-cli --upgrade

# Change absolutely NOTHING.
# DATABRICKS_HOST : It Must Start As https:// : It Must Not End In '/'
# DATABRICKS_TOKEN : It Must Not Be Expired. 


azKeyVaultName=$(az keyvault list -g $param_parameters_resourceGroupName_value --query "[].name" -o tsv)
DATABRICKS_TOKEN=$(az keyvault secret show --name "dbkstoken" --vault-name $AZ_KEYVAULT_NAME --query "value" -o tsv)
echo "Set Databricks Token ID As Environment Variable..."
echo "DATABRICKS_TOKEN=$DATABRICKS_TOKEN" >> $GITHUB_ENV


databricks configure --token 

echo "Test Databricks CLI Commands"
databricks -h 
databricks fs ls

#databricks fs mkdirs dbfs:/tmp/new-dir

