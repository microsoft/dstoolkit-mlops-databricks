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

module azKeyVault '../Az_Resources/Az_KeyVault/Az_KeyVault.bicep' = {
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
module azDataLake '../Az_Resources/Az_DataLake/Az_DataLake.bicep' =  {
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

module logAnalytics '../Az_Resources/Az_AppInsights/Az_AppInsights.bicep' = {
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
// Outputs AzDatabricks Workspace ID, which is used when Assigning RBACs
// ################################################################################################################################################################//
module azMachineLearning'../Az_Resources/Az_Machine_Learning/Az_MachineLearning.bicep' =  {
  dependsOn: [
    logAnalytics
    azDataLake
    azKeyVault

  ]
  scope: resourceGroup(resourceGroupName)
  name: 'azamldbxdstoolkit' 
  params: {
    location: location
    azAppInsightsID: logAnalytics.outputs.azAppInsightsID
    varstorageAccountID: azDataLake.outputs.varstorageAccountID
    azKeyVaultID: azKeyVault.outputs.azKeyVaultID

  }
}

// ################################################################################################################################################################//
//                                                                       Module for Creating Azure Databricks Workspace
// Outputs AzDatabricks Workspace ID, which is used when Assigning RBACs
// ################################################################################################################################################################//
module azDatabricks '../Az_Resources/Az_Databricks/Az_Databricks.bicep' =  {
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



