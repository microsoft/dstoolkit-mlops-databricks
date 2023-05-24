# https://pumpingco.de/blog/run-an-azure-pipelines-job-only-if-source-code-has-changed/
# https://stackoverflow.com/questions/54231548/shallow-fetch-for-repository

echo $PATH_FILTER
echo $VAR_NAME
echo $DevOps_Agent

CHANGED_FILES=$(git diff HEAD HEAD~ --name-only)
MATCH_COUNT=0

echo "Checking for file changes..."
for FILE in $CHANGED_FILES
do
    if [[ $FILE == *$PATH_FILTER* ]]; then
        echo "MATCH:  ${FILE} changed"
        MATCH_COUNT=$(($MATCH_COUNT+1))
    else
        echo "IGNORE: ${FILE} changed"
    fi
done

echo "$MATCH_COUNT match(es) for filter '$PATH_FILTER' found."
if [[ $MATCH_COUNT -gt 0 ]]; then
    if [[ $DevOps_Agent == "GitHub" ]]; then
        echo "Running in GitHub Actions"
        echo "$VAR_NAME=true" >> $GITHUB_ENV
    else
        echo "Running in Azure DevOps"
        echo "##vso[task.setvariable variable="VAR_NAME";isOutput=true;]true"
    fi  
else
    if [[ $DevOps_Agent == "GitHub" ]]; then
        echo "$VAR_NAME=false" >> $GITHUB_ENV
    else
        echo "##vso[task.setvariable variable="VAR_NAME";isOutput=true;]false"
    fi  
fi