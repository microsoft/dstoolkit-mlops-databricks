
param environment string
param location string
var keyVaultName = 'keyvault-${environment}-${substring(uniqueString(resourceGroup().id), 0, 4)}'


resource azKeyVault 'Microsoft.KeyVault/vaults@2021-10-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'premium'
    }
    tenantId: subscription().tenantId
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
      ipRules: []
      virtualNetworkRules: []
    }
    enableRbacAuthorization: true // if this is false then you cannot use RBAC assignments, on acl (below). If true acl (below) is ignored
    enableSoftDelete: false
    enabledForTemplateDeployment: true
    accessPolicies: [
        {
          //applicationId: 'ce79c2ef-170d-4f1c-a706-7814efb94898' // Application ID of databricks SPN
          permissions: {
            // Give it the ability to set secrets // we can then get rid of the Key Vault Admin permission set in the main pipeline
              // Can we do this for the main spn , the equivalent of serviceConnect1
            secrets: [
            'set'
            'list'
            'get'
          ]
          }
          tenantId: subscription().tenantId
          objectId: 'ab926dd1-657d-4bb2-9987-c7857046d0dd'
        }
        
        {
        //applicationId: '5d57ca95-aca6-453d-9110-97f687d9dff6' // Application ID of serviceConnect1
        permissions: {
          secrets: [
            'set'
            'list'
            'get'
          ]
        }
        tenantId: subscription().tenantId
        objectId: '47527038-bf92-4422-8632-961c5851c21b'
      }
    ]
  }
  
}

output azKeyVaultName string = azKeyVault.name
