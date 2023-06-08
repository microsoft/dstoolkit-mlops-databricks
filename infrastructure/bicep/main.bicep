targetScope = 'subscription'

param location string
param environment string
param storageConfig object
param containerNames array
param resourceGroupName string
param workspaceName string
param pricingTier string
param ShouldCreateContainers bool = true
param loganalyticswsname string 
param appInsightswsname string 
param storageAccountName string 
param TemplateParamFilePath string
param TemplateFilePath string
param AZURE_DATABRICKS_APP_ID string
param MANAGEMENT_RESOURCE_ENDPOINT string 
param amlblobname string 
param amlwsname string 

// ################################################################################################################################################################//
//                                                                       Create Resource Group                                                                    
// ################################################################################################################################################################//
resource azResourceGroup 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  dependsOn: []
  name: resourceGroupName
  // Location of the Resource Group Does Not Have To Match That of The Resouces Within. Metadata for all resources within groups can reside in 'uksouth' below
  location: location
}


// ################################################################################################################################################################//
//                                                                  KEY VAULT - SELECT KV                                                                                //
// ################################################################################################################################################################//

module azKeyVault 'az_templates/az_key_vault/az_key_vault.bicep' = {
  dependsOn: [
    azResourceGroup
    
  ]
  scope: azResourceGroup
  name: 'azKeyVault'
  params: {
    environment: environment 
    location: location
  }
}

// ################################################################################################################################################################//
//                                                                       Module for Create Azure Data Lake Storage
// RBAC is assigned -> azDatabricks given access to Storage 
// ################################################################################################################################################################//
module azDataLake 'az_templates/az_data_lake/az_data_lake.bicep' =  {
  dependsOn: [
    azResourceGroup
  ]
  scope: resourceGroup(resourceGroupName)
  name: 'azDataLake' 
  params: {
    storageAccountName: storageAccountName
    storageConfig: storageConfig
    location: location
    containerNames: containerNames
    ShouldCreateContainers: ShouldCreateContainers
    workspaceName: workspaceName
    resourceGroupName: resourceGroupName
    azKeyVaultName: azKeyVault.outputs.azKeyVaultName


  }
}


module logAnalytics 'az_templates/az_app_insights/az_app_insights.bicep' = {
  dependsOn: [
    azResourceGroup
  ]
  scope: resourceGroup(resourceGroupName)
  name: 'logAnalytics'
  params: {
    location: location
    logwsname: loganalyticswsname
    appinsightname: appInsightswsname
  }
}


// ################################################################################################################################################################//
//                                                                       Module for Creating Azure Machine Learning Workspace
// Outputs AzDatabricks Workspace ID, which is used when Assigning RBACs.
// ################################################################################################################################################################//
module azMachineLearning 'az_templates/az_machine_learning/az_machine_learning.bicep' =  {
  dependsOn: [
    logAnalytics
    azDataLake
    azKeyVault

  ]
  scope: resourceGroup(resourceGroupName)
  name: 'amlws' 
  params: {
    location: location
    azAppInsightsID: logAnalytics.outputs.azAppInsightsID
    azKeyVaultID: azKeyVault.outputs.azKeyVaultID
    amlwsname: amlwsname
    amlblobname: amlblobname



  }
}

// ################################################################################################################################################################//
//                                                                       Module for Creating Azure Databricks Workspace
// Outputs AzDatabricks Workspace ID, which is used when Assigning RBACs
// ################################################################################################################################################################//
module azDatabricks 'az_templates/az_databricks/az_databricks.bicep' =  {
  dependsOn: [
    azMachineLearning
  ]
  scope: resourceGroup(resourceGroupName)
  name: 'azDatabricks' 
  params: {
    location: location
    workspaceName: workspaceName
    pricingTier: pricingTier
    azMachineLearningWSId: azMachineLearning.outputs.azMachineLearningWSId
  }
}



output azDatabricksWorkspaceID string = azDatabricks.outputs.azDatabricksWorkspaceID 



