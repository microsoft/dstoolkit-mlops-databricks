#!/bin/bash
echo "Ingest JSON File"
JSON=$( jq '.' MLOps_Engineer/2-Infrastructure_Layer/DBX_CICD_Deployment/Bicep_Params/$ENVIRONMENT/Bicep.parameters.json)

TemplateParamFilePath=$( jq -r '.parameters.TemplateParamFilePath.value' <<< "$JSON")
echo "Resource Group Name: $TemplateParamFilePath"


TemplateFilePath=$( jq -r '.parameters.TemplateFilePath.value' <<< "$JSON")
echo "Resource Group Name: $TemplateFilePath"

location=$( jq -r '.parameters.location.value' <<< "$JSON")
echo "Resource Group Name: $location"


echo "environment variable: $TemplateParamFilePath"
echo "environment variable: $Location"
echo "environment variable: $TemplateFilePath"
# Important to define unique deployment names as conflicts will occur
echo "Create Azure DBX Resource Environments...."

az deployment sub create \
    --location $location \
    --template-file $TemplateFilePath \
    --parameters $TemplateParamFilePath \
    --name "$ENVIRONMENT"