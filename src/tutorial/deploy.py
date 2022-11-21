"""Configure Databricks cluster."""

from pathlib import Path

# import sys
# sys.path.append(str(Path(__file__).parent.parent.joinpath('modules')))
import os
import json
from dbkcore.core import Log
from scripts import create_cluster
from scripts import install_dbkframework
from scripts import set_secrets
from scripts import local_config
import argparse

Log(name=Path(__file__).stem)


def command_exec(command, ignore=False):
    """
    Execute shell command.

    Parameters
    ----------
    command : str
        Command to execute
    ignore : bool, optional
        Ignore exception, by default False

    Raises
    ------
    Exception
        Raises exception if command failes
    """
    Log.get_instance().log_info(f'Running command -> {command}')
    if not ignore:
        if os.system(command) != 0:
            raise Exception(f'Failed to execute: {command}')


def parse_args(args_list=None):
    """
    Parse command line arguments.

    Parameters
    ----------
    args_list : [type], optional
        Argument list, by default None

    Returns
    -------
    ArgumentParser
        Arguments parsed
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', help="Full path of cluster's json configuration", type=str, required=True)
    args_parsed = parser.parse_args(args_list)
    return args_parsed


def main(cluster_config_file):
    """
    Execute the script.

    Parameters
    ----------
    cluster_config_file : str
        Path of the configuration file

    Raises
    ------
    Exception
        Raises when script failes
    """
    appi_key_env = 'APPI_IK'

    create_cluster.main(cluster_config_file=cluster_config_file)
    local_config.main(cluster_config_file=cluster_config_file)
    set_secrets.main(scope='config', secret_name=appi_key_env, secret_value=os.environ[appi_key_env])
    install_dbkframework.main(cluster_config_file=cluster_config_file)


if __name__ == "__main__":
    args = parse_args()
    main(cluster_config_file=args.config_file)
