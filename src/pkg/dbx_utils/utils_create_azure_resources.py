import os
import subprocess
import json

__here__ = os.path.dirname(__file__)

ENVIRONMENT = os.environ['ENVIRONMENT']


class LoadJson():
    def __init__(self):
        self.json_file = 'infrastructure/bicep/params/' + ENVIRONMENT + '/bicep.parameters.json'

    def load_json(self):
        with open(self.json_file, 'r') as f:
            repos_config = json.load(f)
        return repos_config
    
    def get_param_file_path(self):
        return self.load_json()['parameters']['TemplateParamFilePath']['value']
    
    def get_template_file_path(self):
        return self.load_json()['parameters']['TemplateFilePath']['value']
    
    def get_location(self):
        return self.load_json()['parameters']['location']['value']


def run_cmd(cmd):
    #May Need To Rmove shell=True
    process = subprocess.run(cmd, stdout=subprocess.PIPE)
    #print(process)
    output = process.stdout.decode().split('\n')
    #print(output)
    output = [line.strip('\n').strip('\r') for line in output]
    #print(output)
    if process.returncode != 0:
        raise RuntimeError('\n'.join(output))
    return output


def deploy_azure_resources():
    json_obj = LoadJson()
    template_param_file_path = json_obj.get_param_file_path()
    template_file_path = json_obj.get_template_file_path()
    location  = json_obj.get_location()

    az_deploy_cmd = ["az", "deployment", "sub", "create",
                    "--location", location,
                    "--template-file", template_file_path,
                    "--parameters", template_param_file_path,
                    "--name", ENVIRONMENT,
                    "--only-show-errors"  ]
    

    print("Deploying Azure Resources... This Make Take A Few Minutes")
    run_cmd(az_deploy_cmd)


if __name__ == "__main__":
    deploy_azure_resources()
