# Create The Service Principal
# WARNING: DO NOT DELETE OUTPUT

$SubscriptionId=( az account show --query id -o tsv )

$main_sp_name="main_sp_"+$(Get-Random -Minimum 1000 -Maximum 9999)

# use --sdk-auth flag if using GitHub Action Azure Authenticator 
$DBX_CREDENTIALS=( az ad sp create-for-rbac -n $main_sp_name --role Owner --scopes /subscriptions/$SubscriptionId --query "{ARM_TENANT_ID:tenant, ARM_CLIENT_ID:appId, ARM_CLIENT_SECRET:password}")


# Service Principal Credentials
$DBX_CREDENTIALS=( $DBX_CREDENTIALS | convertfrom-json )
#echo $DBX_CREDENTIALS
$Client_ID=( $DBX_CREDENTIALS.ARM_CLIENT_ID )


# Retrieve Object IDs
$main_sp_name_obj_id=( az ad sp show --id $Client_ID --query "{roleBeneficiaryObjID:id}" -o tsv )

echo "Back Stop Command For Older Azure CLI Command"
 
if ($main_sp_name_obj_id -eq "None" ) { $main_sp_name_obj_id=( az ad sp show --id $Client_ID --query "{roleBeneficiaryObjID:objectId}" -o tsv ) }


 
$User_ObjID=( az ad signed-in-user show --query "{roleBeneficiaryObjID:id}" -o tsv )
 
echo "Back Stop Command For Older Azure CLI Command"
 
if ($User_ObjID -eq "None" ) { $User_ObjID=( az ad signed-in-user show --query "{roleBeneficiaryObjID: objectId}" -o tsv ) }
 



echo "Update The Variable Files"
$environments = @('sandbox', 'development', 'uat', 'production')
foreach ($environment in $environments)
{
   $JsonData = Get-Content infrastructure/databricks/databricks_configs/$environment/repos.json -raw | ConvertFrom-Json
   foreach ($Obj in $JsonData.Git_Configuration)
   {
       ($Obj.git_username = $Git_Configuration )
   }
   foreach ($Obj in $JsonData.Repo_Configuration)
   {
       ($Obj.url = $Repo_ConfigurationURL )
   }
   $JsonData | ConvertTo-Json -Depth 4  | set-content infrastructure/databricks/databricks_configs/$environment/repos.json -NoNewline
}
 
foreach ($environment in $environments)
{
  $JsonData = Get-Content infrastructure/databricks/databricks_configs/$environment/rbac.json -raw | ConvertFrom-Json
  $JsonData.RBAC_Assignments | % {if($_.Description -eq 'Your Object ID'){$_.roleBeneficiaryObjID=$User_ObjID}}
  $JsonData.RBAC_Assignments | % {if($_.Description -eq 'Databricks SPN'){$_.roleBeneficiaryObjID=$main_sp_name_obj_id}}
  $JsonData | ConvertTo-Json -Depth 4  | set-content infrastructure/databricks/databricks_configs/$environment/rbac.json -NoNewline
}

git add . 
git commit . -m 'Demo Deployment Commit'

git config core.autocrlf false
git rm --cached -r .
git reset --hard
git push

# Secret Configuration

#clear

echo "Credentials Used In Later Step - Do Not Delete"
echo $DBX_CREDENTIALS