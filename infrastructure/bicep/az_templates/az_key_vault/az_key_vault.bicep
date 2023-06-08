
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
    enableSoftDelete: true
    enabledForTemplateDeployment: true
    accessPolicies: [
        {
          //applicationId: '<>' // Application ID of databricks SPN
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
          objectId: '<>'
        }
        
        {
        //applicationId: '<>' // Application ID of serviceConnect1
        permissions: {
          secrets: [
            'set'
            'list'
            'get'
          ]
        }
        tenantId: subscription().tenantId
        objectId: '<>'
      }
    ]
  }
  
}

output azKeyVaultName string = azKeyVault.name
output azKeyVaultID string = azKeyVault.id
