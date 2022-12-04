REPOS_WITH_MANAGEMENT_PERMISSIONS=$(curl -X GET \
                -H "Authorization: Bearer $DBRKS_BEARER_TOKEN" \
                -H "X-Databricks-Azure-SP-Management-Token: $DBRKS_MANAGEMENT_TOKEN" \
                -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                -H 'Content-Type: application/json' \
                https://$DATABRICKS_INSTANCE/api/2.0/repos )



echo "Display Repos In DBX With Manage Permissions...."
echo $REPOS_WITH_MANAGEMENT_PERMISSIONS

echo "Retrieve Repo ID For ..."
REPO_ID=$( jq -r --arg updateFolder "$updateFolder" ' .repos[] | select( .path | contains($updateFolder)) | .id ' <<< "$REPOS_WITH_MANAGEMENT_PERMISSIONS")

echo "Repo ID: $REPO_ID"

echo "Git Pull on DBX Repo $updateFolder With $branchName Branch "

JSON_STRING=$( jq -n -c --arg tb "$branchName" \
        '{branch: $tb}' )

GIT_PULL_RESPONSE=$(curl -X PATCH \
-H "Authorization: Bearer $DBRKS_BEARER_TOKEN" \
-H "X-Databricks-Azure-SP-Management-Token: $DBRKS_MANAGEMENT_TOKEN" \
-H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
-H 'Content-Type: application/json' \
-d $JSON_STRING \
https://$DATABRICKS_INSTANCE/api/2.0/repos/$REPO_ID )

echo "Git Pull Response..."
echo $GIT_PULL_RESPONSE
