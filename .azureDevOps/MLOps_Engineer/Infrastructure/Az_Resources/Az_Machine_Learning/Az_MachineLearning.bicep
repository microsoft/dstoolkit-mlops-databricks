param location string 
param azAppInsightsID string
param varstorageAccountID string 
param azKeyVaultID string

resource AzMachineLearning 'Microsoft.MachineLearningServices/workspaces@2022-12-01-preview' = {
  name: 'azamldbxdstoolkit'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
    applicationInsights: azAppInsightsID
    storageAccount: varstorageAccountID
    keyVault: azKeyVaultID

  }
  sku: {
    name: 'standard'

  }
  
}


output azMachineLearningWSId string = AzMachineLearning.id
