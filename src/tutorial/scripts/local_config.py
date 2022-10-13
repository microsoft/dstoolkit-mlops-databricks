import sys
from pathlib import Path

# sys.path.append(str(Path(__file__).parent.parent.joinpath('modules')))
import json
from dbkcore.core import Log
from dbkenv.core import ResourceClient
from dbkenv.core import Configuration
from dbkenv.core import DatabricksResourceManager
from dbkenv.local import DatabricksLocal
import argparse




Log(name=Path(__file__).stem)


def parse_args(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', help="Full path of cluster's json configuration", type=str, required=True)
    args_parsed = parser.parse_args(args_list)
    return args_parsed


def main(cluster_config_file):

    configuration = Configuration(file_load=True)
    # cluster_config_file = str(Path(__file__).parent.joinpath('unittest_cluster.json'))

    with open(cluster_config_file.strip(), 'r') as cl:
        cluster_configuration = json.load(cl)

    cluster_name = cluster_configuration['cluster_name']

    print("Cluster Name")

    print(cluster_name)

    client = ResourceClient(
        host=configuration.DATABRICKS_HOST,
        personal_token=configuration.DATABRICKS_TOKEN
    )
    print("Client")
    print(client)
    drm = DatabricksResourceManager(
        client=client,
        cluster_name=cluster_name,
        cluster_configuration=cluster_configuration
    )

    cluster_id = drm.cluster.cluster_id

    print(cluster_id)

    local_config = DatabricksLocal(
        host=configuration.DATABRICKS_HOST,
        databricks_token=configuration.DATABRICKS_TOKEN,
        cluster_id=cluster_id,
        org_id=configuration.DATABRICKS_ORDGID
    )
    local_config.initialize()


if __name__ == "__main__":
    args = parse_args()
    main(cluster_config_file=args.config_file)
