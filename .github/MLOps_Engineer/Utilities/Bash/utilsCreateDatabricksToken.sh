SECRET_NAME="dbkstoken"
# Check if secret exists
SECRET_EXISTS=$(az keyvault secret list \
                --vault-name $AZ_KEYVAULT_NAME \
                --query "contains([].id, \
                'https://$AZ_KEYVAULT_NAME.vault.azure.net/secrets/$SECRET_NAME')")

echo "secret exists: $SECRET_EXISTS"

if [ $SECRET_EXISTS == true ]; then
    echo "Secret '$SECRET_NAME' exists! fetching..."
    DATABRICKS_TOKEN=$(az keyvault secret show \
                    --name $SECRET_NAME \
                    --vault-name $AZ_KEYVAULT_NAME \
                    --query "value" \
                    -o tsv )

    echo "Secret Value: $DATABRICKS_TOKEN"
    
    echo "Databricks Token As Environment Variable..."
    echo "##vso[task.setvariable variable="DATABRICKS_TOKEN";isOutput=true;]$DATABRICKS_TOKEN"

else
    echo "Secret '$SECRET_NAME' Do Not exist! Creating PAT Token & Store In Key Vault..."
    # Must Assign SP Minimum Contributor Permissions. Must also give the SP Key Vault Administrator Privileges (Need to Set these in YAML)

    PAT_TOKEN_RESPONSE=$(curl -X POST \
                        -H "Authorization: Bearer $DBRKS_BEARER_TOKEN" \
                        -H "X-Databricks-Azure-SP-Management-Token: $DBRKS_MANAGEMENT_TOKEN" \
                        -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" -d \
                        '{
                            "lifetime_seconds": "30000000", 
                            "comment": "Token For Databricks"
                        }' https://$DATABRICKS_INSTANCE/api/2.0/token/create )

    echo "PAT Token Creation Response...."
    echo $PAT_TOKEN_RESPONSE

    DATABRICKS_TOKEN=$(jq .token_value -r <<< "$PAT_TOKEN_RESPONSE")
    echo "PAT Token Creation...."
    echo $DATABRICKS_TOKEN

    echo "Store PAT In Key Vault...."
    az keyvault secret set \
        --vault-name $AZ_KEYVAULT_NAME \
        --name $SECRET_NAME \
        --value $DATABRICKS_TOKEN
    
    echo "Databricks Token As Environment Variable..."
    echo "##vso[task.setvariable variable="DATABRICKS_TOKEN";isOutput=true;]$DATABRICKS_TOKEN"
fi
