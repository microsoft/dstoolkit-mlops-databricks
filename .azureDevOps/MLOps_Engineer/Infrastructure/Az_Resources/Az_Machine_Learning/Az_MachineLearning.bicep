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

resource amlci 'Microsoft.MachineLearningServices/workspaces/computes@2020-09-01-preview' = {
  parent: AzMachineLearning
  name: 'aml-cluster'
  location: location
  properties: {
    computeType: 'AmlCompute'
    properties: {
      vmSize: 'Standard_DS3_v2'
      subnet: json('null')
      osType: 'Linux'
      scaleSettings: {
        maxNodeCount: 2
        minNodeCount: 0
      }
    }
  }
}
output azMachineLearningWSId string = AzMachineLearning.id
