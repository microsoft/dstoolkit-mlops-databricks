APP_INSIGHT_INSTRUMENT_KEY=$(az resource show \
                            -g databricks-dev-rg \
                            -n dbxappinsightsdev \
                            --resource-type "microsoft.insights/components" \
                            --query properties.ConnectionString -o tsv)


echo "Creating Secret Scopes...."

echo "Create DBX_SP_Credentials Scope...."

Create_Secret_Scope=$(curl -X POST -H "Authorization: Bearer $TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' -d \
                            '{
                                "scope": "DBX_SP_Credentials", 
                                "initial_manage_principal": "users" 
                            }' https://$DATABRICKS_INSTANCE/api/2.0/secrets/scopes/create )

echo "Inserting Service Principal + Other Secrets Into Scope.... "

Create_DBX_Client_Secret=$(curl -X POST -H "Authorization: Bearer $TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' -d \
                            '{
                                "scope": "DBX_SP_Credentials", 
                                "key": "DBX_SP_Client_Secret",
                                "string_value": "$ARM_CLIENT_SECRET"
                            }' https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )
                            
Create_DBX_ClientID_Secret=$(curl -X POST \
                            -H "Authorization: Bearer $TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' -d \
                            '{
                                "scope": "DBX_SP_Credentials", 
                                "key": "DBX_SP_ClientID",
                                "string_value": "$ARM_CLIENT_ID"
                            }' https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )

Create_DBX_TenantID_Secret=$(curl -X POST -H "Authorization: Bearer $TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' -d \
                            '{
                                "scope": "DBX_SP_Credentials", 
                                "key": "DBX_SP_TenantID",
                                "string_value": "$ARM_TENANT_ID"
                            }' https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )


echo "Create Azure Resources Secrets Scope...."

Create_Secret_Scope=$(curl -X POST -H "Authorization: Bearer $TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' -d \
                            '{
                                "scope": "AzureResourceSecrets", 
                                "initial_manage_principal": "users" 
                            }' https://$DATABRICKS_INSTANCE/api/2.0/secrets/scopes/create )

#There can be encoding problems passing some variables directly into the api request. Use json_String below with jq to solve this issue
JSON_STRING=$( jq -n -c --arg scope "AzureResourceSecrets" --arg key "appi_ik" --arg value "$APP_INSIGHT_INSTRUMENT_KEY"  \
                            '{
                                scope: $scope,
                                key: $key,
                                string_value: $value
                            }' )

Create_APP_INSIGHT_INSTRUMENT_KEY_SecretD=$(curl -X POST \
                            -H "Authorization: Bearer $TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            -d $JSON_STRING \
                            https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )



