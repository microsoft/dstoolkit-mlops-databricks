#!/bin/bash

echo $ENVIRONMENT
echo "Ingest JSON File"
JSON=$( jq '.' .azureDevOps/MLOps_Engineer/Infrastructure/DBX_CICD_Deployment/Bicep_Params/$ENVIRONMENT/Bicep.parameters.json)

TemplateParamFilePath=$( jq -r '.parameters.TemplateParamFilePath.value' <<< "$JSON")
echo "Parm File Path: $TemplateParamFilePath"


TemplateFilePath=$( jq -r '.parameters.TemplateFilePath.value' <<< "$JSON")
echo "File Path: $TemplateFilePath"

location=$( jq -r '.parameters.location.value' <<< "$JSON")
echo "Location: $location"


echo "environment variable: $TemplateParamFilePath"
echo "environment variable: $location"
echo "environment variable: $TemplateFilePath"
# Important to define unique deployment names as conflicts will occur
echo "Create Azure DBX Resource Environments...."

az deployment sub create \
    --location $location \
    --template-file $TemplateFilePath \
    --parameters $TemplateParamFilePath \
    --name "$ENVIRONMENT" \
    --only-show-errors