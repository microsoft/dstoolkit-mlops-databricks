
###################################################################################################################################################################//
##                                                                      Create Key Vault Secrets                                               
###################################################################################################################################################################//
SECRET_VALUE=$ARM_CLIENT_ID
SECRET_NAME="ARMCLIENTID"

SECRET_EXISTS=$(az keyvault secret list \
                --vault-name $AZ_KEYVAULT_NAME \
                --query "contains([].id, \
                'https://$AZ_KEYVAULT_NAME.vault.azure.net/secrets/$SECRET_NAME')")

echo "secret exists: $SECRET_EXISTS"

if [ $SECRET_EXISTS == true ]; then
    echo "Secret '$SECRET_NAME' exists! fetching..."
    SECRET_VALUE=$(az keyvault secret show \
                    --name $SECRET_NAME \
                    --vault-name $AZ_KEYVAULT_NAME \
                    --query "value")

    echo "Secret Value: $SECRET_VALUE"

else
    echo "Secret '$SECRET_NAME' Do Not exist! Creating PAT Token & Store In Key Vault..."
    # Must Assign SP Minimum Contributor Permissions. Must also give the SP Key Vault Administrator Privileges (Need to Set these in YAML)
    echo "Store Secret In Key Vault...."
    az keyvault secret set \
        --vault-name $AZ_KEYVAULT_NAME \
        --name $SECRET_NAME \
        --value $SECRET_VALUE
fi


###################################################################################################################################################################//
##                                                                      ARM_TENANT                                               
###################################################################################################################################################################//


SECRET_VALUE=$ARM_TENANT_ID
SECRET_NAME="ARMTENANTID"
# Check if secret exists
SECRET_EXISTS=$(az keyvault secret list \
                --vault-name $AZ_KEYVAULT_NAME \
                --query "contains([].id, \
                'https://$AZ_KEYVAULT_NAME.vault.azure.net/secrets/$SECRET_NAME')")

echo "secret exists: $SECRET_EXISTS"

if [ $SECRET_EXISTS == true ]; then
    echo "Secret '$SECRET_NAME' exists! fetching..."
    SECRET_VALUE=$(az keyvault secret show \
                    --name $SECRET_NAME \
                    --vault-name $AZ_KEYVAULT_NAME \
                    --query "value")

    echo "Secret Value: $SECRET_VALUE"

else
    echo "Secret '$SECRET_NAME' Do Not exist! Creating PAT Token & Store In Key Vault..."
    # Must Assign SP Minimum Contributor Permissions. Must also give the SP Key Vault Administrator Privileges (Need to Set these in YAML)
    echo "Store Secret In Key Vault...."
    az keyvault secret set \
        --vault-name $AZ_KEYVAULT_NAME \
        --name $SECRET_NAME \
        --value $SECRET_VALUE
fi


###################################################################################################################################################################//
##                                                                      ARM_Client_Secret                                               
###################################################################################################################################################################//


SECRET_VALUE=$ARM_CLIENT_SECRET
SECRET_NAME="ARMCLIENTSECRET"
# Check if secret exists
SECRET_EXISTS=$(az keyvault secret list \
                --vault-name $AZ_KEYVAULT_NAME \
                --query "contains([].id, \
                'https://$AZ_KEYVAULT_NAME.vault.azure.net/secrets/$SECRET_NAME')")

echo "secret exists: $SECRET_EXISTS"

if [ $SECRET_EXISTS == true ]; then
    echo "Secret '$SECRET_NAME' exists! fetching..."
    SECRET_VALUE=$(az keyvault secret show \
                    --name $SECRET_NAME \
                    --vault-name $AZ_KEYVAULT_NAME \
                    --query "value")

    echo "Secret Value: $SECRET_VALUE"

else
    echo "Secret '$SECRET_NAME' Do Not exist! Creating PAT Token & Store In Key Vault..."
    # Must Assign SP Minimum Contributor Permissions. Must also give the SP Key Vault Administrator Privileges (Need to Set these in YAML)
    echo "Store Secret In Key Vault...."
    az keyvault secret set \
        --vault-name $AZ_KEYVAULT_NAME \
        --name $SECRET_NAME \
        --value $SECRET_VALUE
fi