LIST_CLUSTERS=$(curl -X GET -H "Authorization: Bearer $TOKEN" \
                    -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                    -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                    -H 'Content-Type: application/json' \
                    https://$DATABRICKS_INSTANCE/api/2.0/clusters/list )

echo "List Clusters"
echo $LIST_CLUSTERS


CLUSTER_NAMES=$( jq -r '[.clusters[].cluster_name]' <<< "$LIST_CLUSTERS")

echo "CLUSTER_NAMES"
echo $CLUSTER_NAMES

echo "Ingest JSON Environment File"
JSON=$( jq '.' .github/workflows/Pipeline_Param/$environment.json)
#echo "${JSON}" | jq

echo "JSON File"
echo $JSON

REPO_URL=$( jq -r '.Repo_Configuration[].url ' <<< $JSON )

echo "REPO_URL"
echo $REPO_URL

GIT_PROVIDER=$( jq -r '.Repo_Configuration[].provider ' <<< $JSON )

echo "GIT_PROVIDER"
echo $GIT_PROVIDER



for row in $(echo "${JSON}" | jq -r '.Jobs[] | @base64'); do
    _jq() {
        echo ${row} | base64 --decode | jq -r ${1}
    }

    echo "in for loop"

    CLUSTER_ID=$( jq -r  '.clusters[] | select( .cluster_name == "dbx-sp-cluster" ) | .cluster_id ' <<< "$LIST_CLUSTERS")
    
    echo "CLUSTER_ID"
    echo $CLUSTER_ID
    
    JSON_STRING=$( jq -n -c \
                    --arg name "$(_jq '.name')" \
                    --arg cluster_name "$(_jq '.cluster_name')" \
                    --arg notebook_path "$(_jq '.notebook_task.notebook_path')"  \
                    --arg source "$(_jq '.notebook_task.source')" \
                    --arg git_branch "$(_jq '.git_branch')" \
                    --arg git_provider "$GIT_PROVIDER" \
                    --arg git_url "$REPO_URL" \
                    --arg CLUSTER_ID "$CLUSTER_ID" \
                    '{
                        "name": $name,
                        "git_source": {
                            "git_url": $git_url,
                            "git_provider": $git_provider,
                            "git_branch": $git_branch
                        },
                        "tasks": [ 
                            {
                                "task_key": $name,
                                "existing_cluster_id": $CLUSTER_ID,
                                "notebook_task": { "notebook_path": $notebook_path, "source": "GIT" }
                            } 
                        ]
                    }' )

    
    echo $JSON_STRING



    CREATE_JOB=$(curl -X POST -H "Authorization: Bearer $TOKEN" \
            -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
            -H 'Content-Type: application/json' \
            -d $JSON_STRING \
            https://$DATABRICKS_INSTANCE/api/2.1/jobs/create )

    echo $CREATE_JOB
    
done


### IGNORE BELOW: Experimental Piece


#echo 'clusterID'
#clusterId=$( jq -r  '.clusters[] | select( .cluster_name | contains("dbx-sp-cluster")) | .cluster_id ' <<< "$listClusters")
#echo $clusterId
#"0609-130637-9rhcw0m1"
# Below - The Job ID has hypens 0609-130637-9rhcw0m1 and has issues when you pass it to the api below. It is PARAMOUNT to use double quoutes
# and single quote around the variable so that it evaluates correctly. 

#createDatabricksJob=$(curl -X POST -H "Authorization: Bearer $token" -H "X-Databricks-Azure-SP-Management-Token: $mgmt_access_token" -H "X-Databricks-Azure-Workspace-Resource-Id: $wsId" -H 'Content-Type: application/json' -d \
#'{
#"name": "unittestjob",
#"existing_cluster_id": "'$clusterId'" ,
#"notebook_task": {"notebook_path": "/Users/ce79c2ef-170d-4f1c-a706-7814efb94898/unittest"}
#}' https://$workspaceUrl/api/2.1/jobs/create )


#listJobs=$(curl -X GET -H "Authorization: Bearer $token" -H "X-Databricks-Azure-SP-Management-Token: $mgmt_access_token" -H "X-Databricks-Azure-Workspace-Resource-Id: $wsId" -H 'Content-Type: application/json' https://$workspaceUrl/api/2.1/jobs/list )
#echo 'List Jobs'
#echo $listJobs

#jobID=$( jq -r  '.jobs[] | select( .settings.name | contains("unittestjob")) | .job_id ' <<< "$listJobs")
#854685009836639
#echo 'List JobID'
#echo $jobID


#runJob=$(curl -X POST -H "Authorization: Bearer $token" -H "X-Databricks-Azure-SP-Management-Token: $mgmt_access_token" -H "X-Databricks-Azure-Workspace-Resource-Id: $wsId" -H 'Content-Type: application/json' -d \
#'{
#"job_id": "'$jobID'"
#}' https://$workspaceUrl/api/2.1/jobs/run-now )

#echo 'List runJob'
#echo $runJob

#echo 'Create Secret Scope'
#echo $createDatabricksJob 













