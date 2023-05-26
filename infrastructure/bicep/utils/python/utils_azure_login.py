"""
    This script is used to login to Azure using a service principal
"""
import os
import subprocess


ARM_CLIENT_ID = os.environ['ARM_CLIENT_ID']
ARM_CLIENT_SECRET = os.environ['ARM_CLIENT_SECRET']
ARM_TENANT_ID = os.environ['ARM_TENANT_ID']


def run_cmd(cmd):
    """
        Run a command and return the output as a list of lines
        shell=false for devops pipelines
    """
    process = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, shell=False)
    output = process.stdout.decode().split('\n')
    #print(output)
    output = [
        line.strip('\n').strip('\r').strip('"') for line in output
        if line.strip('\n').strip('\r')
    ]
    #import pdb; pdb.set_trace()
    #print(f"Return Code: {process.returncode}").
    if process.returncode != 0:
        raise RuntimeError('\n'.join(output))
    return output, process.returncode


def start_azure_login():
    """
        Login to Azure using the service principal
    """
    az_login_cmd = ["az", "login", "--service-principal",
                    "-u", ARM_CLIENT_ID,
                    "-p", ARM_CLIENT_SECRET,
                    "--tenant", ARM_TENANT_ID
                    ]
    print("Logging In To Azure")
    #_, returncode = run_cmd(az_login_cmd)
    output, returncode = run_cmd(az_login_cmd)
    return returncode


if __name__ == '__main__':
    returncode = start_azure_login()
    print(f"Return Code: {returncode}")
