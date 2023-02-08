
AAD_TOKEN=$( az account get-access-token \
            --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
            --query "accessToken" \
            --output tsv )

echo $AAD_TOKEN

CREATE_REPO_RESPONSE=$(curl -X POST -H "Authorization: Bearer $AAD_TOKEN" \
            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
            -H 'Content-Type: application/scim+json' \
            -d $JSON_STRING \
            '{
                "displayName": "My Service Principal",
                "applicationId": "12a34b56-789c-0d12-e3fa-b456789c0123",
                "entitlements": [
                    {
                    "value": "allow-cluster-create"
                    }
                ],
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"
                ],
                "active": true
            }' https://$DATABRICKS_INSTANCE/api/2.0/preview/scim/v2/ServicePrincipals )






