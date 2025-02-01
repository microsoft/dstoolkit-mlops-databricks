![Banner](docs/images/MLOps_for_databricks_Solution_Acclerator_logo.JPG)



 ## MLOps Gify Visual (Allow 20 Seconds To Load)

![](https://github.com/ciaran28/dstoolkit-mlops-databricks/blob/main/docs/images/Azure_Machine_Learning_GIF.gif) 

---
 <br>


 # Version History And Updates 

## Azure ML Integration Now Live For GitHub Deployment.
Development for Azure DevOps Deployment in Progress

 # MLOps for Databricks with CI/CD (GitHub Actions)
---
 
 ## MLOps Architecture

![image](https://user-images.githubusercontent.com/108273509/207945308-14e4794e-e86b-4bee-aa21-088698983703.png)

Features to be included in future releases:
- Model testing
- Metrics & Monitoring 
---


## Youtube Demo - Slightly Outdated

The deployment instructions for the video are slightly outdated (albeit still useful). 
Please follow instructions below instead. The video still provides useful content for concepts outwith the deployment. 

[![Youtube Demo](docs/images/YoutubeThumbNail.png)](https://youtu.be/g57N3GYXuDI)

---

## About This Repository

This Repository contains an Azure Databricks Continuous Deployment _and_ Continuous Development Framework for delivering Data Engineering/Machine Learning projects based on the below Azure Technologies:



| Azure Databricks | Azure Log Analytics | Azure Monitor Service  | Azure Key Vault        |
| ---------------- |:-------------------:| ----------------------:| ----------------------:|



Azure Databricks is a powerful technology, used by Data Engineers and Scientists ubiquitously. However, operationalizing it within a Continuous Integration and Deployment setup that is fully automated, may prove challenging. 

The net effect is a disproportionate amount of the Data Scientist/Engineers time contemplating DevOps matters. This Repository's guiding vision is to automate as much of the infrastructure as possible.

---
---

## Prerequisites
<details open>
<summary>Click Dropdown... </summary>
<br>
  
- Github Account
- Microsoft Azure Subscription
- VS Code
- Azure CLI Installed (This Accelerator is tested on version 2.39)

</details>

---
---

## Details of The Solution Accelerator

- Creation of four environments:
  - Sandbox
  - Development 
  - User Acceptance Testing (UAT)
  - Production
- Full CI/CD between environments
- Infrastructure-as-Code for interacting with Databricks API and also CLI
- Azure Service Principal Authentication
- Azure resource deployment using BICEP
- Databricks Feature Store + MLFlow Tracking + Model Registry + Model Experiments
- DBX by Data Labs for Continuous Deployment of Jobs/Workflows (source code/ parameters files packaged within DBFS)


---
# Deployment Instructions 

## Create Repository
<details open>
<summary>Click Dropdown... </summary>
<br>
  
- Fork this repository [here](https://github.com/microsoft/dstoolkit-mlops-databricks/fork) 
- In your Forked Repo, click on 'Actions' and then 'Enable'
- Within your VS Code click, "View", then "Command Pallette", "Git: Clone", and finally select your Repo
</details>

---
 
## Login To Azure
- All Code Throughout To Go Into VS Code **PowerShell Terminal** 

 ```ps
az login

# If There Are Multiple Tenants In Your Subscription, Ensure You Specify The Correct Tenant "az login --tenant"

# ** Microsoft Employees Use: az login --tenant fdpo.onmicrosoft.com (New Non Prod Tenant )

```

## GitHub Account
```ps
echo "Enter Your Git Username... "
# Example: "Ciaran28"
$Git_Configuration = "GitHub_Username"
```

## GitHub Repos Within Databricks
  ```ps
echo "Enter Your Git Repo Url (this could be any Repository In Your Account )... "
# Example: "https://github.com/ciaran28/dstoolkit-mlops-databricks" 
$Repo_ConfigurationURL = ""
```

## Updates Parameter Files & Git Push To Remote
  ```ps
echo "From root execute... "

./setup.ps1


```
---

## Create Environments 
Follow the naming convention (case sensitive)
<img width="971" alt="image" src="https://user-images.githubusercontent.com/108273509/205917146-a7deb2ae-674a-4ec1-a9b8-4859bcdce25f.png">


## Secrets

**For each environment** create GitHub Secrets entitled **ARM_CLIENT_ID**, **ARM_CLIENT_SECRET** and **ARM_TENANT_ID** using the output in VS Code PowerShell Terminal from previous step.
(Note: The Service Principal below was destroyed, and therefore the credentials are useless )

<img width="656" alt="image" src="https://user-images.githubusercontent.com/108273509/194619649-2ef7e325-a6bb-4760-9a82-1e3b4775adbd.png">

In addition generate a GitHub Personal Access Token and use it to create a secret named **PAT_GITHUB**:

<img width="883" alt="image" src="https://user-images.githubusercontent.com/108273509/205918329-9592e20f-439b-4e1b-b7c4-983579e295de.png">
 
---
---


 
## Final Snapshot of GitHub Secrets

Secrets in GitHub should look exactly like below. The secrets are case sensitive, therefore be very cautious when creating. 

<img width="587" alt="image" src="https://user-images.githubusercontent.com/108273509/205921220-9ad2116a-7c85-4725-a70c-e178a0af2914.png">


---
---
 
## Deploy The Azure Environments 

- In GitHub you can manually run the pipeline to deploy the environments to Azure using "onDeploy.yaml" found [here](.github/workflows/onDeploy.yaml). Use the instructions below to run the workflow.

<img width="893" alt="image" src="https://user-images.githubusercontent.com/108273509/205954210-c123c407-4c83-4952-ab4b-cd6c485efc2f.png">

- Azure Resources created (Production Environment snapshot - For speed I have hashed out all environment deployments except Sandbox. Update onDeploy.yaml to deploy all environments)
  
<img width="1175" alt="image" src="https://user-images.githubusercontent.com/108273509/194638664-fa6e1809-809e-45b2-9655-9312f32f24bb.png">


---
---

## Running Pipelines

- The end to end machine learning pipeline will be pre-configured in the "workflows" section in databricks. This utilises a Job Cluster which will automatically upload the necessary dependencies contained within a python wheel file 

- If you wish to run the machine learning scripts from the Notebook instead, first upload the dependencies (automatic upload is in development). Simply navigate to python wheel file contained within the dist/ folder. Manually upload the python wheel file to the cluster that you wish to run for the Notebook. 

---
---

 ## Continuous Deployment And Branching Strategy

The Branching Strategy I have chosen is configured automatically as part of the accelerator. It follows a GitHub Flow paradigm in order to facilitate rapid Continuous Integration, with some nuances. (see Footnote 1 which contains the SST Git Flow Article written by Willie Ahlers for the Data Science Toolkit - This provides a narrative explaining the numbers below)[^1]


The branching strategy is easy to change via updating the "if conditions" within .github/workflows/onRelease.yaml.

<img width="805" alt="image" src="https://user-images.githubusercontent.com/108273509/186166011-527144d5-ebc1-4869-a0a6-83c5538b4521.png">

-   Pull Request from Feature Branch to Main Branch: C.I Tests
-   Pull Request approved from Feature Branch to Main Branch: C.D. to Development Environment 
-   Pull Request from Main Branch to Release Branch: C.I. Test
-   Pull Request approved from Main Branch to Release Branch: C.D. to User Acceptance Testing (UAT) Environment
-   Tag Version and Push to Release Branch: C.D. to Production Environment 
- Naming conventions for branches (to ensure the CD pipelines will deploy - onRelease.yaml for more details ):
  - Feature Branches: "feature/<insertname>"
  - Main Branch: "main"
  - Release branch "release/<insertname>"

---
---
## MLOps Paradigm: Deploy Code, not Models

In most situations, Databricks recommends that during the ML development process, you promote code, rather than models, from one environment to the next. Moving project assets this way ensures that all code in the ML development process goes through the same code review and integration testing processes. It also ensures that the production version of the model is trained on production code. For a more detailed discussion of the options and trade-offs, see Model deployment patterns.

https://learn.microsoft.com/en-us/azure/databricks/machine-learning/mlops/deployment-patterns

<img width="427" alt="image" src="https://user-images.githubusercontent.com/108273509/211937862-2aaf118f-85c1-4d98-af75-56628837ffa4.png">


---
---
## Feature Store Integration 

In an organization, thousands of features are buried in different scripts and in different formats; they are not captured, organized, or preserved, and thus cannot be reused and leveraged by teams other than those who generated them.

Because feature engineering is so important for machine learning models and features cannot be shared, data scientists must duplicate their feature engineering efforts across teams.

To solve those problems, a concept called feature store was developed, so that:

- Features are centralized in an organization and can be reused
- Features can be served in real-time with low latency

![image](https://user-images.githubusercontent.com/108273509/216114586-0c4dea68-a98c-4cf6-938a-ceecf11b12a8.png)

