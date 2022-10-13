
LIST_CLUSTERS=$(curl -X GET -H "Authorization: Bearer $TOKEN" \
                    -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                    -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                    -H 'Content-Type: application/json' \
                    https://$DATABRICKS_INSTANCE/api/2.0/clusters/list )

# Extract Existing Cluster Names
CLUSTER_NAMES=$( jq -r '[.clusters[].cluster_name]' <<< "$LIST_CLUSTERS")

echo "Ingest JSON Environment File"
JSON=$( jq '.' .github/workflows/Pipeline_Param/$environment.json)
#echo "${JSON}" | jq

echo "Configure All Clusters From Environment Parameters File"
for row in $(echo "${JSON}" | jq -r '.Clusters[] | @base64'); do
    _jq() {
        echo ${row} | base64 --decode | jq -r ${1}
    }
    if [[ ! " ${CLUSTER_NAMES[*]} " =~ "$(_jq '.cluster_name')" ]]; then

        echo "Cluster Does Not Exist: Create Cluster... "
        
        JSON_STRING=$( jq -n -c \
                    --arg cn "$(_jq '.cluster_name')" \
                    --arg sv "$(_jq '.spark_version')" \
                    --arg nt "$(_jq '.node_type_id')"  \
                    --arg nw "$(_jq '.autoscale.max_workers')" \
                    --arg sc "$(_jq '.spark_conf')" \
                    --arg at "$(_jq '.autotermination_minutes')" \
                    '{cluster_name: $cn,
                    spark_version: $sv,
                    node_type_id: $nt,
                    num_workers: ($nw|tonumber),
                    autotermination_minutes: ($at|tonumber),
                    spark_conf: ($sc|fromjson)}' )
        
        CREATE_CLUSTER=$(curl -X POST -H "Authorization: Bearer $TOKEN" \
                    -H "X-Databricks-Azure-SP-Management-Token: $MGMT_ACCESS_TOKEN" \
                    -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                    -H 'Content-Type: application/json' \
                    -d $JSON_STRING \
                    https://$DATABRICKS_INSTANCE/api/2.0/clusters/create )

    else
        echo "Cluster Exists... Ignore"  
    fi
done

