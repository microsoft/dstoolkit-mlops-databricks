"""Create a cluster in Databricks."""
import sys
from pathlib import Path

# sys.path.append(str(Path(__file__).parent.parent.joinpath('modules')))
import json
from dbkenv.core import DatabricksResourceManager
from dbkenv.core import Configuration
from dbkenv.core import ResourceClient
from dbkenv.core import Log
import argparse




Log(name=Path(__file__).stem)


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
    configuration = Configuration(file_load=True)
    # cluster_config_file = str(Path(__file__).parent.joinpath('unittest_cluster.json'))

    with open(cluster_config_file.strip(), 'r') as cl:
        cluster_configuration = json.load(cl)

    cluster_name = cluster_configuration['cluster_name']
    # instantiate the logger

    client = ResourceClient(
        host=configuration.DATABRICKS_HOST,
        personal_token=configuration.DATABRICKS_TOKEN
    )
    drm = DatabricksResourceManager(
        client=client,
        cluster_name=cluster_name,
        cluster_configuration=cluster_configuration
    )

    drm.cluster.create_cluster_and_wait()


if __name__ == "__main__":
    args = parse_args()
    main(cluster_config_file=args.config_file)
