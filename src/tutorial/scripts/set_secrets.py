"""
Add secret to Databricks.
"""
import sys
from pathlib import Path

# sys.path.append(str(Path(__file__).parent.parent.joinpath('modules')))
from dbkenv.core import Configuration
from dbkenv.core import ResourceClient
from dbkenv.core import Secret
from dbkenv.core import Log
import argparse


Log(name=Path(__file__).stem)


def parse_args(args_list=None):
    """
    Parse command line arguments.

    Parameters
    ----------
    args_list : [type], optional
        [description], by default None

    Returns
    -------
    ArgumentParser
        Parsed arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', help="Scope to use", type=str, required=True)
    parser.add_argument('--secret_name', help="Name of the secret", type=str, required=True)
    parser.add_argument('--secret_value', help="Value of the secret", type=str, required=True)
    args_parsed = parser.parse_args(args_list)
    return args_parsed


def main(
    scope: str,
    secret_name: str,
    secret_value: str
):
    """
    Run main function.

    Parameters
    ----------
    scope : str
        Scope to use
    secret_name : str
        Name of the secret
    secret_value : str
        Value of the secret
    """
    configuration = Configuration(file_load=True)

    client = ResourceClient(
        host=configuration.DATABRICKS_HOST,
        personal_token=configuration.DATABRICKS_TOKEN
    )
    secret_client = Secret(
        client=client
    )

    scopes = secret_client.scopes()
    if scope not in scopes:
        secret_client.add_scope(
            scope=scope
        )

    secret_client.add_secret(
        scope=scope,
        secret_name=secret_name,
        secret_value=secret_value
    )


if __name__ == "__main__":
    args = parse_args()
    main(
        scope=args.scope,
        secret_name=args.secret_name,
        secret_value=args.secret_value
    )
    # main(
    #     scope='test_scope',
    #     secret_name='test_name',
    #     secret_value='test_value'
    # )
