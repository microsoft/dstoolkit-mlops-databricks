import os
from dbkcore.core import Log
from scripts import set_secrets

appi_key_env = 'APPI_IK'
set_secrets.main(scope='config', secret_name=appi_key_env, secret_value=os.environ[appi_key_env])