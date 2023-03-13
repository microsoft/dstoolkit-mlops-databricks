param location string 

resource AzMachineLearning 'Microsoft.MachineLearningServices/workspaces@2022-12-01-preview' = {
  name: 'azamldbxdstoolkit'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

output azMachineLearningWSId string = AzMachineLearning.id
