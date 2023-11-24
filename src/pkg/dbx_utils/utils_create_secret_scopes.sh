az config set extension.use_dynamic_install=yes_without_promp
az extension add --name application-insights

echo $RESOURCE_GROUP_NAME
echo $DATABRICKS_INSTANCE
echo $WORKSPACE_ID
echo $SUBSCRIPTION_ID

APP_INSIGHT_NAME=$(az resource list \
                -g $RESOURCE_GROUP_NAME \
                --resource-type 'microsoft.insights/components' \
                --query [].name \
                -o tsv )

APP_INSIGHT_INSTRUMENT_KEY=$( az monitor app-insights component show \
                            -g $RESOURCE_GROUP_NAME \
                            -a $APP_INSIGHT_NAME \
                            --query connectionString )

echo "Test"

echo $APP_INSIGHT_NAME
echo $APP_INSIGHT_INSTRUMENT_KEY
echo $SUBSCRIPTION_ID

echo "Creating Secret Scopes...."

echo "Create DBX_SP_Credentials Scope...."

Create_Secret_Scope=$(curl -X POST -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' -d \
                            '{
                                "scope": "DBX_SP_Credentials", 
                                "initial_manage_principal": "users" 
                            }' https://$DATABRICKS_INSTANCE/api/2.0/secrets/scopes/create )

echo "Inserting Service Principal + Other Secrets Into Scope.... "




JSON_STRING=$( jq -n -c \
                --arg scope "DBX_SP_Credentials" \
                --arg key "DBX_SP_Client_Secret" \
                --arg value "$ARM_CLIENT_SECRET"  \
                '{
                    scope: $scope,
                    key: $key,
                    string_value: $value
                }' )

echo $JSON_STRING

Create_DBX_Client_Secret=$(curl -X POST -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            -d $JSON_STRING \
                            https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )



JSON_STRING=$( jq -n -c \
                --arg scope "DBX_SP_Credentials" \
                --arg key "DBX_SP_ClientID" \
                --arg value "$ARM_CLIENT_ID"  \
                '{
                    scope: $scope,
                    key: $key,
                    string_value: $value
                }' )
echo $JSON_STRING
                                        
Create_DBX_ClientID_Secret=$(curl -X POST \
                            -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            -d $JSON_STRING \
                            https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )



JSON_STRING=$( jq -n -c --arg scope "DBX_SP_Credentials" --arg key "DBX_SP_TenantID" --arg value "$ARM_TENANT_ID"  \
                            '{
                                scope: $scope,
                                key: $key,
                                string_value: $value
                            }' )

echo $JSON_STRING

Create_DBX_TenantID_Secret=$(curl -X POST -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            -d $JSON_STRING \
                            https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )



JSON_STRING=$( jq -n -c --arg scope "DBX_SP_Credentials" --arg key "SUBSCRIPTION_ID" --arg value "$SUBSCRIPTION_ID"  \
                            '{
                                scope: $scope,
                                key: $key,
                                string_value: $value
                            }' )

echo $JSON_STRING

CREATE_SUBSCRIPTIONID=$(curl -X POST -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            -d $JSON_STRING \
                            https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )





echo "Create Azure Resources Secrets Scope...."

Create_Secret_Scope=$(curl -X POST -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
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

Create_APP_INSIGHT_INSTRUMENT_KEY_Secret=$(curl -X POST \
                            -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            -d $JSON_STRING \
                            https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )


JSON_STRING=$( jq -n -c --arg scope "AzureResourceSecrets" --arg key "RESOURCE_GROUP_NAME" --arg value "$RESOURCE_GROUP_NAME"  \
                            '{
                                scope: $scope,
                                key: $key,
                                string_value: $value
                            }' )

CREATE_RESOURCE_GROUP_NAME_SECRET=$(curl -X POST \
                            -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            -d $JSON_STRING \
                            https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )


JSON_STRING=$( jq -n -c --arg scope "AzureResourceSecrets" --arg key "AML_WS_NAME" --arg value "$AML_WS_NAME"  \
                            '{
                                scope: $scope,
                                key: $key,
                                string_value: $value
                            }' )

CREATE_AML_WS_NAME_SECRET=$(curl -X POST \
                            -H "Authorization: Bearer $DATABRICKS_AAD_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DATABRICKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            -d $JSON_STRING \
                            https://$DATABRICKS_INSTANCE/api/2.0/secrets/put )
