import os
import requests
from azureml.core import Workspace, Experiment
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.pipeline.steps import PythonScriptStep, DatabricksStep
from azureml.pipeline.core import Pipeline, PipelineData, StepSequence
from azureml.core.compute import ComputeTarget, DatabricksCompute
from azureml.exceptions import ComputeTargetException
from azureml.core.authentication import ServicePrincipalAuthentication


DATABRICKS_COMPUTE_NAME = os.environ['DATABRICKS_COMPUTE_NAME']
DATABRICKS_CLUSTER_NAME = os.environ['DATABRICKS_CLUSTER_NAME']
RESOURCE_GROUP_NAME = os.environ['RESOURCE_GROUP_NAME']
DATABRICKS_AAD_TOKEN = os.environ['DATABRICKS_AAD_TOKEN']
DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
DATABRICKS_WS_NAME = os.environ['DATABRICKS_WS_NAME']
WORKSPACE_ID = os.environ['WORKSPACE_ID']
SUBSCRIPTION_ID = os.environ['SUBSCRIPTION_ID']
ARM_CLIENT_SECRET = os.environ['ARM_CLIENT_SECRET']
ARM_TENANT_ID = os.environ['ARM_TENANT_ID']
ARM_CLIENT_ID = os.environ['ARM_CLIENT_ID']
DATABRICKS_MANAGEMENT_TOKEN = os.environ['DATABRICKS_MANAGEMENT_TOKEN']
ENVIRONMENT = os.environ['ENVIRONMENT']
AML_WS_NAME = os.environ['AML_WS_NAME']

DBRKS_REQ_HEADERS = {
    'Authorization': f'Bearer {DATABRICKS_AAD_TOKEN}',
    'X-Databricks-Azure-SP-Management-Token': f'{DATABRICKS_MANAGEMENT_TOKEN}',
    'X-Databricks-Azure-Workspace-Resource-Id': f'{WORKSPACE_ID}',
    'Content-Type': 'application/json'
}

print(DATABRICKS_COMPUTE_NAME)
print(WORKSPACE_ID)
print(RESOURCE_GROUP_NAME)
print(DATABRICKS_AAD_TOKEN)
print(DATABRICKS_MANAGEMENT_TOKEN)
print(DATABRICKS_INSTANCE)
print(SUBSCRIPTION_ID)
print(ARM_CLIENT_SECRET)
print(ARM_TENANT_ID)
print(ARM_CLIENT_ID)
print(AML_WS_NAME)

class GetClusterID():
    def __init__(self, cluster_name):
        self.clusters_obj = self.list_clusters()['clusters']
        self.cluster_name = cluster_name
    def get_cluster_id(self):
        """
            Returns Cluster ID for a given cluster name.
        """
        for cluster in self.clusters_obj:
            if cluster['cluster_name'] ==  self.cluster_name:
                print("ml_cluster exists")
                cluster_id = cluster['cluster_id']
                print(cluster_id)
                return cluster_id
    def list_clusters(self):
        """
            Returns a Json object containing a list of existing Databricks Clusters.
        """
        response = requests.get('https://' + DATABRICKS_INSTANCE + '/api/2.0/clusters/list', headers=DBRKS_REQ_HEADERS)
        if response.status_code != 200:
            raise Exception(response.content)
        else:
            return response.json()

def create_pipeline_structure(databricks_compute, ws, cluster_id):
    print('Creating the pipeline structure')

    nyc_taxi_e2e_mlops = DatabricksStep(
        name="nyc_taxi_e2e_mlops",
        notebook_path="/Repos/"+ ARM_CLIENT_ID + "/Sandbox/data_science/src_nyc_taxi/src.py",
        #notebook_params={'myparam': 'testparam', 
        #    'myparam2': pipeline_param},
        run_name='nyc_taxi_e2e_mlops',
        compute_target=databricks_compute,
        existing_cluster_id=cluster_id,
        allow_reuse=True,
        num_workers=3
    )


    step_sequence = StepSequence(steps=[nyc_taxi_e2e_mlops])
    pipeline = Pipeline(workspace=ws, steps=step_sequence)
    pipeline.validate()
    
    return pipeline


if __name__ == "__main__":
    svc_pr = ServicePrincipalAuthentication(
        tenant_id = ARM_TENANT_ID,
        service_principal_id = ARM_CLIENT_ID,
        service_principal_password = ARM_CLIENT_SECRET 
    )
    ws = Workspace(
        subscription_id=SUBSCRIPTION_ID,
        resource_group=RESOURCE_GROUP_NAME,
        workspace_name=AML_WS_NAME,
        auth=svc_pr
    )

    print(f" AML Workspace Properties: {ws} ")

    try:
        databricks_compute = DatabricksCompute(workspace=ws, name=DATABRICKS_COMPUTE_NAME)
        print('Compute target {} already exists'.format(DATABRICKS_COMPUTE_NAME))
    except ComputeTargetException:
        print('Compute not found, will use below parameters to attach new one')
        print('db_compute_name {}'.format(DATABRICKS_COMPUTE_NAME))
        print('db_resource_group {}'.format(RESOURCE_GROUP_NAME))
        print('db_workspace_name {}'.format(DATABRICKS_WS_NAME))
        print('db_access_token {}'.format(DATABRICKS_AAD_TOKEN))

        config = DatabricksCompute.attach_configuration(
            resource_group = RESOURCE_GROUP_NAME,
            workspace_name = DATABRICKS_WS_NAME,
            access_token= DATABRICKS_AAD_TOKEN)
        databricks_compute=ComputeTarget.attach(ws, DATABRICKS_COMPUTE_NAME, config)
        databricks_compute.wait_for_completion(True)

    
    cluster_obj = GetClusterID("ml_cluster")
    cluster_id = cluster_obj.get_cluster_id()



    #existingClusters = listClusters()['clusters']
    #for cluster in existingClusters:
    #    if cluster['cluster_name'] == "ml_cluster":
    #        print("ml_cluster exists")
    #        cluster_id = cluster['cluster_id']
    #        print(cluster_id)
    #    else:
    #        print("ml_cluster does not exist: cannot continue")
    #notebook_path=os.getenv("DATABRICKS_NOTEBOOK_PATH", "/Data_Scientist/featureEngineering.py")
    #notebook_path=os.getenv("DATABRICKS_NOTEBOOK_PATH", "databricks.ipynb")


    pipeline = create_pipeline_structure(databricks_compute=databricks_compute, ws=ws, cluster_id=cluster_id)
    published_pipeline = pipeline.publish("databricks_pipeline", version="1.0.0", description="Databricks Pipeline")


