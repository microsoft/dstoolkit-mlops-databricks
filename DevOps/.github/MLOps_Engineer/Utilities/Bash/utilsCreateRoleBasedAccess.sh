#!/usr/bin/env bash


echo "Resource Group Name: $RESOURCE_GROUP_NAME"
echo "ENVIRONMENT: $ENVIRONMENT"
RESOURCE_GROUP_ID=$( az group show -n $RESOURCE_GROUP_NAME --query id -o tsv )

echo "Ingest JSON File"
JSON=$( jq '.' .github/MLOps_Engineer/Variables/$ENVIRONMENT/RBAC.json)
#echo "${JSON}" | jq

for row in $(echo "${JSON}" | jq -r '.RBAC_Assignments[] | @base64'); do
    _jq() {
        echo ${row} | base64 --decode | jq -r ${1}
    }
    ROLES_ARRAY="$(_jq '.roles')"
    echo $ROLES_ARRAY

    # Before: [ "Contributor", "DBX_Custom_Role", "Key Vault Administrator" ]
    # xargs trims whitespace on either side. -n removes newline characters.
    ROLES_ARRAY_PARSED=$( echo $ROLES_ARRAY | jq -r | tr -d "[]" | tr -d \'\" | xargs echo -n )
    # After: Contributor, DBX_Custom_Role, Key Vault Administrator
    #echo $ROLES_ARRAY_PARSED
    Field_Separator=$IFS
    IFS=,
    for ROLE in $ROLES_ARRAY_PARSED; do
        ROLE=$( echo $ROLE | xargs )
        
        az role assignment create \
        --role "$ROLE" \
        --assignee-object-id $(_jq '.roleBeneficiaryObjID') \
        --assignee-principal-type "$(_jq '.principalType')" \
        --scope "$RESOURCE_GROUP_ID"
        #--scope "$(_jq '.scope')"

    done    
    IFS=$Field_Separator
done