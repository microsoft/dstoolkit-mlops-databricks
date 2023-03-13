param location string 
param azAppInsightsID string
param azKeyVaultID string


resource azBlob 'Microsoft.Storage/storageAccounts@2021-08-01' =  {    
  name: 'blobstorage'
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

resource AzMachineLearning 'Microsoft.MachineLearningServices/workspaces@2022-12-01-preview' = {
  name: 'azamldbxdstoolkit'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
    applicationInsights: azAppInsightsID
    storageAccount: azBlob.id
    keyVault: azKeyVaultID

  }
  sku: {
    name: 'standard'

  }
  
}


output azMachineLearningWSId string = AzMachineLearning.id
