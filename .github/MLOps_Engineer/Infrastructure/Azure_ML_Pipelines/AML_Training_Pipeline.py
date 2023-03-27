import os
from azureml.core import Workspace, Experiment
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.pipeline.steps import PythonScriptStep, DatabricksStep
from azureml.pipeline.core import Pipeline, PipelineData, StepSequence
from azureml.core.compute import ComputeTarget, DatabricksCompute
from azureml.exceptions import ComputeTargetException


DATABRICKS_COMPUTE_NAME = os.environ['DATABRICKS_COMPUTE_NAME']
RESOURCE_GROUP_NAME = os.environ['RESOURCE_GROUP_NAME']
DATABRICKS_TOKEN = os.environ['DATABRICKS_TOKEN']
DATABRICKS_INSTANCE = os.environ['DATABRICKS_INSTANCE']
SUBSCRIPTION_ID = os.environ['SUBSCRIPTION_ID']
ARM_CLIENT_SECRET = os.environ['ARM_CLIENT_SECRET']
ARM_TENANT_ID = os.environ['ARM_TENANT_ID']
ARM_CLIENT_ID = os.environ['ARM_CLIENT_ID']
AML_WS_NAME = os.environ['AML_WS_NAME']

print(DATABRICKS_COMPUTE_NAME)
print(RESOURCE_GROUP_NAME)
print(DATABRICKS_TOKEN)
print(DATABRICKS_INSTANCE)
print(SUBSCRIPTION_ID)
print(ARM_CLIENT_SECRET)
print(ARM_TENANT_ID)
print(ARM_CLIENT_ID)
print(AML_WS_NAME)


def create_pipeline_structure(compute_target: ComputeTarget, workspace: Workspace):
    print('Creating the pipeline structure')

    Databricks_Featurization_Step = DatabricksStep(
        name="Databricks_Feature_Engineering",
        notebook_path="/Repos/841ba6d9-a509-44ee-bf40-c0876b4ac6bb/Sandbox/Data_Scientist/featureEngineering",
        #notebook_params={'myparam': 'testparam', 
        #    'myparam2': pipeline_param},
        run_name='Databricks_Feature_Engineering',
        compute_target=databricks_compute,
        existing_cluster_id="0323-095026-ibph8gox",
        allow_reuse=True
    )

    Databricks_Model_Training = DatabricksStep(
        name="Databricks_Model_Training",
        
        notebook_path="/Workspace/Repos/841ba6d9-a509-44ee-bf40-c0876b4ac6bb/Sandbox/Data_Scientist/modelTraining",
        #notebook_params={'myparam': 'testparam', 
        #    'myparam2': pipeline_param},
        run_name='Databricks_Model_Training',
        compute_target=databricks_compute,
        existing_cluster_id="0323-095026-ibph8gox",
        allow_reuse=True
    )

    step_sequence = StepSequence(steps=[Databricks_Featurization_Step, Databricks_Model_Training])
    pipeline = Pipeline(workspace=workspace, steps=step_sequence)
    pipeline.validate()
    

    return pipeline

import os
from azureml.core.authentication import ServicePrincipalAuthentication

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
    DATABRICKS_COMPUTE_NAME = DatabricksCompute(workspace=ws, name=DATABRICKS_COMPUTE_NAME)
    print('Compute target {} already exists'.format(DATABRICKS_COMPUTE_NAME))

except ComputeTargetException:
    print('Compute not found, will use below parameters to attach new one')
    print('db_compute_name {}'.format(DATABRICKS_COMPUTE_NAME))
    print('db_resource_group {}'.format(RESOURCE_GROUP_NAME))
    print('db_workspace_name {}'.format(DATABRICKS_INSTANCE))
    print('db_access_token {}'.format(DATABRICKS_TOKEN))

    config = DatabricksCompute.attach_configuration(
        resource_group = RESOURCE_GROUP_NAME,
        workspace_name = DATABRICKS_INSTANCE,
        access_token= DATABRICKS_TOKEN)
    databricks_compute=ComputeTarget.attach(ws, DATABRICKS_COMPUTE_NAME, config)
    databricks_compute.wait_for_completion(True)

#
#notebook_path=os.getenv("DATABRICKS_NOTEBOOK_PATH", "/Data_Scientist/featureEngineering.py")
notebook_path=os.getenv("DATABRICKS_NOTEBOOK_PATH", "databricks.ipynb")

pipeline = create_pipeline_structure(compute_target=databricks_compute,  workspace=ws)

published_pipeline = pipeline.publish("databricks_pipeline", version="1.0.0", description="Databricks Pipeline")


