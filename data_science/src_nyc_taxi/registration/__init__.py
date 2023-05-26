# When this is called, we will promote the most recent logged version to the production stage (register and stage)

# The Model That gets logged to the production stage, is then also deployed to Azure Kubernetes in AML. But This only happens if log to aml
# is true

# Data Drift monitoring run in AML - It will trigger the dbx pipeline, (log to aml will be set to true) - and when there is an improvement 
# the model is promoted to production in databricks, and then deploy (PROBLEM MLFLOW NEEDS TO REGISTER TO BOTH DATABRICKS REGISTRY, AND THEN K8S)

# TWO STEP PROCESS. 

# MLFLOW allways logs to databricks. Regardless if pipeline is run from AML or Databricks 

# If a model makes it to production stage, then it is logged to aml also (server will have to be switched) - Do this at the model deployment step
# We then deploy the model to K8s. Production in Databricks will always tell us what is serving in AML. 