param location string 
param azAppInsightsID string
param azKeyVaultID string
param amlwsname string 
param amlblobname string 


var varamlblobname = '${amlblobname}${substring(uniqueString(resourceGroup().id), 0, 4)}'
var varamlwsname = '${amlwsname}-${substring(uniqueString(resourceGroup().id), 0, 4)}'


resource amlBlob 'Microsoft.Storage/storageAccounts@2021-08-01' =  {    
  name: varamlblobname
    location: location
    kind: 'StorageV2'
    sku: {
      name: 'Standard_LRS'
    }
    properties: {
      allowBlobPublicAccess: true
      isHnsEnabled: false
      accessTier: 'Hot'
    }
}


resource AzMachineLearning 'Microsoft.MachineLearningServices/workspaces@2023-04-01' = {
  name: varamlwsname
  location: location

  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
    applicationInsights: azAppInsightsID
    storageAccount: amlBlob.id
    keyVault: azKeyVaultID
  }
  
  sku: {
    name: 'Enterprise'
  }
    
}

output azMachineLearningWSId string = AzMachineLearning.id
