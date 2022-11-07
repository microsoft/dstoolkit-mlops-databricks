"""
Various utilities for speed up development
"""
import os
from pathlib import Path
import json
from typing import Any




def current_directory() -> str:
    """
    Get current directory.

    Returns
    -------
    str
        The current directory path
    """
    return os.path.dirname(os.path.realpath(__file__))


def add_folder_in_current_directory(folder_name: str) -> bool:
    """
    Add a folder in the current directory.

    Parameters
    ----------
    folder_name : str
        New folder name

    Returns
    -------
    bool
        True if success
    """
    output_folder = os.path.join(current_directory(), folder_name)
    os.makedirs(output_folder)
    return True


def is_json_serializable(x: Any) -> bool:
    """
    Check if the object is serializable.

    Parameters
    ----------
    x : Any
        Object to validate

    Returns
    -------
    bool
        True if success
    """
    try:
        json.dumps(x)
        return True
    except Exception:
        return False



# TODO: remove
# def load_envs(current_file: Path):
#     """
#     Helper function for local development

#     Parameters
#     ----------
#     current_file : Path
#         Paht of the current file
#     """
#     from dotenv import load_dotenv

#     root_folder = "analytics"
#     found_env = False
#     base_env = current_file.parent.absolute()

#     while not found_env:
#         matches = [f for f in base_env.glob("*.env")]
#         # print(matches)
#         if matches:
#             env_file = [f for f in base_env.glob("*.env")][0]
#             print(env_file)
#             load_dotenv(env_file, override=True, verbose=True)
#             found_env = True
#             print("Environment file found")
#         elif base_env.name == root_folder:
#             break
#         else:
#             base_env = base_env.parent


