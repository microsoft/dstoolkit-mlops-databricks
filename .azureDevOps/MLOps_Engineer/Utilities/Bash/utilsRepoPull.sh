REPOS_WITH_MANAGEMENT_PERMISSIONS=$(curl -X GET \
                -H "Authorization: Bearer $DBRKS_BEARER_TOKEN" \
                -H "X-Databricks-Azure-SP-Management-Token: $DBRKS_MANAGEMENT_TOKEN" \
                -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                -H 'Content-Type: application/json' \
                https://$DATABRICKS_INSTANCE/api/2.0/repos )



echo "Ingest JSON File"
JSON=$( jq '.' .azureDevOps/MLOps_Engineer/Variables/$ENVIRONMENT/Repos.json)
for row in $(echo "${JSON}" | jq -r '.Repo_Configuration[] | @base64'); do
    _jq() {
        echo ${row} | base64 --decode | jq -r ${1}
    }
    
    echo "PULL_BRANCH: $PULL_BRANCH"
    UPDATE_FOLDER=$(_jq '.path')
    echo "UPDATE FOLDER: $UPDATE_FOLDER"

    if [ -z "$PULL_BRANCH" ];
    then
        PULL_BRANCH=$RELEASE_BRANCH
        "Use Release Branch: $PULL_BRANCH"
    fi

    echo "Display Repos In DBX With Manage Permissions...."
    echo $REPOS_WITH_MANAGEMENT_PERMISSIONS

    echo "Retrieve Repo ID For ..."
    REPO_ID=$( jq -r --arg UPDATE_FOLDER "$UPDATE_FOLDER" ' .repos[] | select( .path | contains($UPDATE_FOLDER)) | .id ' <<< "$REPOS_WITH_MANAGEMENT_PERMISSIONS")

    echo "Repo ID: $REPO_ID"

    echo "Git Pull on DBX Repo $UPDATE_FOLDER With $PULL_BRANCH Branch "

    JSON_STRING=$( jq -n -c --arg tb "$PULL_BRANCH" \
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
done











