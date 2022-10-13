echo "Ingest JSON File"
JSON=$( jq '.' .github/workflows/Pipeline_Param/$environment.json)
#echo "${JSON}" | jq

for row in $(echo "${JSON}" | jq -r '.Git_Configuration[] | @base64'); do
    _jq() {
        echo ${row} | base64 --decode | jq -r ${1}
    }
    echo $PAT_GITHUB
    JSON_STRING=$( jq -n -c \
                --arg pat "$PAT_GITHUB" \
                --arg gu "$(_jq '.git_username')" \
                --arg gp "$(_jq '.git_provider')"  \
                --arg br "$(_jq '.branch')"  \
                '{personal_access_token: $pat,
                git_username: $gu,
                git_provider: $gp,
                branch: $br}' )


    CREATE_GIT_CREDENTIALS_RESPONSE=$(curl -X POST -H "Authorization: Bearer $TOKEN" \
                -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                -H 'Content-Type: application/json' \
                -d $JSON_STRING \
                https://$DATABRICKS_INSTANCE/api/2.0/git-credentials )

    echo "Git Credentials Response...."
    echo $CREATE_GIT_CREDENTIALS_RESPONSE
    
done

echo "User Folders In Databricks Repos Will Be Described Using An Email Address... e.g Ciaranh@Microsoft.com  "
echo "The DevOps Agent SP Which Is Also A User, However Its Databricks Repo User Folder is Named After The AppID: $ARM_CLIENT_ID"
echo "All Folders Defined In The JSON Parameters Folder Will Be Appended To /Repos/<AppId>/"

for row in $(echo "${JSON}" | jq -r '.Repo_Configuration[] | @base64'); do
    _jq() {
        echo ${row} | base64 --decode | jq -r ${1}
    }

    JSON_STRING=$( jq -n -c \
                    --arg url "$(_jq '.url')" \
                    --arg pr "$(_jq '.provider')" \
                    --arg pa "/Repos/$ARM_CLIENT_ID/$(_jq '.path')"  \
                    '{url: $url,
                    provider: $pr,
                    path: $pa}' )
    
    #echo "JSON -D String "
    #echo $JSON_STRING

    CREATE_REPO_RESPONSE=$(curl -X POST -H "Authorization: Bearer $TOKEN" \
                -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                -H 'Content-Type: application/json' \
                -d $JSON_STRING \
                https://$DATABRICKS_INSTANCE/api/2.0/repos )

    echo "Repo Response"
    echo $CREATE_REPO_RESPONSE
done
