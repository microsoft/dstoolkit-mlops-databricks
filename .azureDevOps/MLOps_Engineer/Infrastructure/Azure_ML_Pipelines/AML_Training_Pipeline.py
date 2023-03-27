import os
from azureml.core import Workspace, Experiment
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.pipeline.steps import PythonScriptStep, DatabricksStep
from azureml.pipeline.core import Pipeline, PipelineData, StepSequence
from azureml.core.compute import ComputeTarget, DatabricksCompute
from azureml.exceptions import ComputeTargetException



def get_or_create_compute(compute_name, workspace: Workspace):
    print('Acquiring a compute resource')

    if compute_name in workspace.compute_targets:
        compute_target = workspace.compute_targets[compute_name]
        if compute_target and type(compute_target) is AmlCompute:
            print(f'Using existing compute: {compute_name}')
    else:
        print(f'Creating new compute: {compute_name}')
        provisioning_config = AmlCompute.provisioning_configuration(
            vm_size = 'Standard_DS11_v2',
            min_nodes = 0, max_nodes = 2,
            idle_seconds_before_scaledown=900
        )

        compute_target = ComputeTarget.create(workspace, compute_name, provisioning_config)
        compute_target.wait_for_completion(show_output=True)

    return compute_target


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



get_or_create_compute(compute_name="mlcluster2", workspace = ws)


db_compute_name=os.getenv("DATABRICKS_COMPUTE_NAME", "mlcluster2") # Databricks compute name
db_resource_group=os.getenv("DATABRICKS_RESOURCE_GROUP", "databricks-sandbox-rg") # Databricks resource group
db_workspace_name=os.getenv("DATABRICKS_WORKSPACE_NAME", "dbxwssandbox-eco3") # Databricks workspace name
db_access_token=os.getenv("DATABRICKS_ACCESS_TOKEN", "dapi63c057791ec3ea61ff5190568fe205d6-3") # Databricks access token

try:
    databricks_compute = DatabricksCompute(workspace=ws, name=db_compute_name)
    print('Compute target {} already exists'.format(db_compute_name))
except ComputeTargetException:
    print('Compute not found, will use below parameters to attach new one')
    print('db_compute_name {}'.format(db_compute_name))
    print('db_resource_group {}'.format(db_resource_group))
    print('db_workspace_name {}'.format(db_workspace_name))
    print('db_access_token {}'.format(db_access_token))

    config = DatabricksCompute.attach_configuration(
        resource_group = db_resource_group,
        workspace_name = db_workspace_name,
        access_token= db_access_token)
    databricks_compute=ComputeTarget.attach(ws, db_compute_name, config)
    databricks_compute.wait_for_completion(True)

#
#notebook_path=os.getenv("DATABRICKS_NOTEBOOK_PATH", "/Data_Scientist/featureEngineering.py")
notebook_path=os.getenv("DATABRICKS_NOTEBOOK_PATH", "databricks.ipynb")

ws = Workspace.from_config()
print(ws)

pipeline = create_pipeline_structure(compute_target=databricks_compute,  workspace=ws)

published_pipeline = pipeline.publish("databricks_pipeline", version="1.0.0", description="Databricks Pipeline")


