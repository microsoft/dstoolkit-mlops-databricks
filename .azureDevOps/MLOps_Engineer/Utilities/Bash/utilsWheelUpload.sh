# ISSUE -  There seems to be a delay for the DBFS Folders to Propagate through - Which is therefore leaving no home for the python wheel file. 

# Wait until DBFS Folder is created. And then Upload. Keep iterating etc. 



# The Script Will Ingest Parameters File In Order To Determine Location Of Setup.py Files.
# Each Setup.py Relates To The Creation Of A New Wheel File, Which Will Be Saved In 
# DBFS In A Folder Corresponding To The Cluster The Wheel File Is To Be Uploaded To. 

echo "Import Wheel Dependencies"
python -m pip install --upgrade pip
python -m pip install flake8 pytest pyspark pytest-cov requests
pip3 install -r ./src/pipelines/dbkframework/requirements.txt
python -m pip install --user --upgrade setuptools wheel
sudo apt-get install pandoc


echo "Ingest JSON Environment File"
JSON=$( jq '.' .github/MLOps_Engineer/Variables/$ENVIRONMENT/PyWheel.json)
#echo "${JSON}" | jq


for row in $(echo "${JSON}" | jq -r '.WheelFiles[] | @base64'); do
    _jq() {
        echo ${row} | base64 --decode | jq -r ${1}
    }

    CLUSTER_NAME=$(_jq '.wheel_cluster')
    setup_py_file_path=$(_jq '.setup_py_file_path')
    # We Are Removing Setup.py From The FilePath 'setup_py_file_path'
    root_dir_file_path=${setup_py_file_path%/*}
    
    echo "Wheel File Destined For Cluster: $CLUSTER_NAME "
    echo "Location Of Setup.py File For Wheel File Creation; $setup_py_file_path"
    
    cd $root_dir_file_path

    # Create The Wheel File
    python setup.py sdist bdist_wheel
    
    cd dist 
    ls
    wheel_file_name=$( ls -d -- *.whl )
    echo "Wheel File Name: $wheel_file_name"

    # Install Wheel File
    echo "$root_dir_file_path/dist/$wheel_file_name"
    pip uninstall -y $wheel_file_name
    pip install $wheel_file_name

    # Upoload Wheel File To DBFS Folder. Wheel File Will Be Stored In A Folder Relating To The Cluster
    # It Is To Be Deployed To

    databricks fs rm dbfs:/FileStore/$CLUSTER_NAME/$wheel_file_name
    echo "$root_dir_file_path/dist/$wheel_file_name"
    echo "dbfs:/FileStore/$CLUSTER_NAME/$wheel_file_name"

    # Databricks CLI Does Not Accept Absolute FilePaths, Only Relative
    databricks fs cp $wheel_file_name dbfs:/FileStore/$CLUSTER_NAME/$wheel_file_name --overwrite
    
    # Cleanup - Remove dist folder from DevOps Agent
    pip uninstall -y $wheel_file_name
    cd ..
    rm -rf dist

    upload_to_cluster=$(_jq '.upload_to_cluster?')
    if [ upload_to_cluster ]
    then

        LIST_CLUSTERS=$(curl -X GET -H "Authorization: Bearer $DBRKS_BEARER_TOKEN" \
                            -H "X-Databricks-Azure-SP-Management-Token: $DBRKS_MANAGEMENT_TOKEN" \
                            -H "X-Databricks-Azure-Workspace-Resource-Id: $WORKSPACE_ID" \
                            -H 'Content-Type: application/json' \
                            https://$DATABRICKS_INSTANCE/api/2.0/clusters/list )
        echo $LIST_CLUSTERS

        # Extract Existing Cluster Names
        CLUSTER_NAMES=$( jq -r '[.clusters[].cluster_name]' <<< "$LIST_CLUSTERS")
        echo $CLUSTER_NAMES

        # UPDATE THE CLUSTER NAME SEARCH ===> CREATE A CLUSTER LIST 
        echo 'clusterID'
        echo $CLUSTER_NAME
        CLUSTER_ID=$( jq -r --arg CLUSTER_NAME "$CLUSTER_NAME" ' .clusters[] | select( .cluster_name == $CLUSTER_NAME ) | .cluster_id ' <<< "$LIST_CLUSTERS")
        echo $CLUSTER_ID
        
        #Start Cluster And Wait!

        ## THERE IS A BUG HERE POSSIBLE AT CLUSTER_STATUS = "RUNNING"
        CLUSTER_STATUS=$(databricks clusters get --cluster-id $CLUSTER_ID | jq -r .state)
        echo $CLUSTER_STATUS
        if [ "$CLUSTER_STATUS" == "TERMINATED" ]
        then    
            echo "Cluster $CLUSTER_ID Is TERMINATED... Turning On.... "
            databricks clusters start --cluster-id "$CLUSTER_ID"

        elif [ "$CLUSTER_STATUS" == "RUNNING" ]
        then
            echo "Cluster $CLUSTER_ID already running, skipping..."
        else
            echo "Cluster Is Pending... "
        fi

        #=======================================================
        # Now loop (stay here) until running
        #======================================================

        CLUSTER_STATUS=$(databricks clusters get --cluster-id $CLUSTER_ID | jq -r .state)
        
        while [ "$CLUSTER_STATUS" != "RUNNING" ]
        do
            sleep 30
            echo "Starting..."
            CLUSTER_STATUS=$(databricks clusters get --cluster-id $CLUSTER_ID | jq -r .state)
        done
        echo "Running now..."

        databricks libraries uninstall --cluster-id "$CLUSTER_ID" --whl dbfs:/FileStore/$CLUSTER_NAME/$wheel_file_name
        databricks libraries install --cluster-id $CLUSTER_ID --whl dbfs:/FileStore/$CLUSTER_NAME/$wheel_file_name
        
        # Make This More Effecient. Restart Clusters Once Only After All DBFS Files Have Been Uploaded. Preferably A Restart All Clusters In 
        # The Workspace Would Do. 
        
        databricks clusters restart --cluster-id $CLUSTER_ID
    fi
done



