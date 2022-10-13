#!/bin/bash

echo "environment variable: $param_TemplateParamFilePath"
echo "environment variable: $param_Location"
echo "environment variable: $param_TemplateFilePath"
# Important to define unique deployment names as conflicts will occur
echo "Create Azure DBX Resource Environments...."

az deployment sub create \
    --location $param_Location \
    --template-file $param_TemplateFilePath \
    --parameters $param_TemplateParamFilePath \
    --name "$environment"