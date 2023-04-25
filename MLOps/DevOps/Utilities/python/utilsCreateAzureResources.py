import os
import subprocess
import json

__here__ = os.path.dirname(__file__)

ENVIRONMENT = os.environ['ENVIRONMENT']


def load_json_params():
    with open( 'MLOps/DevOps/Infrastructure/DBX_CICD_Deployment/Bicep_Params/' + ENVIRONMENT + '/Bicep.parameters.json', 'r') as f:
        repos_config = json.load(f)
    return repos_config

def run_cmd(cmd):
    #May Need To Rmove shell=True
    process = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)
    print(process)
    output = process.stdout.decode().split('\n')
    output = [line.strip('\n').strip('\r') for line in output]
    print(output)
    if process.returncode != 0:
        raise RuntimeError('\n'.join(output))
    return output

def deploy_azure_resources():
    template_param_file_path = load_json_params()['parameters']['TemplateParamFilePath']['value']
    template_file_path = load_json_params()['parameters']['TemplateFilePath']['value']
    location = load_json_params()['parameters']['location']['value']

    az_deploy_cmd = ["az", "deployment", "sub", "create", "--location", location, "--template-file", template_file_path, "--parameters", template_param_file_path, "--name", ENVIRONMENT, "--only-show-errors"  ]
    
    print("Deploying Azure Resources... This Make Take A Few Minutes")
    run_cmd(az_deploy_cmd)

def main():
    deploy_azure_resources()

if __name__ == "__main__":
    main()
