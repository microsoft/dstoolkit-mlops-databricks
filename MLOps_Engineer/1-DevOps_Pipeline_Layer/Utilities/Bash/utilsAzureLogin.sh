
az upgrade

az config set extension.use_dynamic_install=yes_without_prompt

az login --service-principal -u $ARM_CLIENT_ID -p $ARM_CLIENT_SECRET --tenant $ARM_TENANT_ID

az account list