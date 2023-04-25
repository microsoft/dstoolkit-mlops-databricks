import os
import subprocess

__here__ = os.path.dirname(__file__)

ARM_CLIENT_ID = os.environ['ARM_CLIENT_ID']
ARM_CLIENT_SECRET = os.environ['ARM_CLIENT_SECRET']
ARM_TENANT_ID = os.environ['ARM_TENANT_ID']


def run_cmd(cmd):
    #May Need To Rmove shell=True
    process = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)
    output = process.stdout.decode().split('\n')
    output = [line.strip('\n').strip('\r') for line in output]

    print(f"Return Code: {process.returncode}")
    if process.returncode != 0:
        raise RuntimeError('\n'.join(output))
    return output

def azure_login():
    #az_login_cmd = ["az", "login", "--tenant", "fdpo.onmicrosoft.com"]
    az_login_cmd = ["az", "login", "--service-principal", "-u", ARM_CLIENT_ID, "-p", ARM_CLIENT_SECRET, "--tenant", ARM_TENANT_ID]
    print("Logging In To Azure")
    run_cmd(az_login_cmd)

def main():
    azure_login()
    
if __name__ == '__main__':
    main()
