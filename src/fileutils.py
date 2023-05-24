"""
Fileutils Module
----------------
This module contains functions for general file management of unspecified type.

It includes:

    - get_files(path, suffix): provides a list of all files with a specified suffix within a folder and all sub-folders.

    - pick_random_csv(path, random_state): picks a random csv file from a folder incl. all sub-folder to work with as a sample.
    
    - save_without_overwrite(data, file_patch): function to save a file without overwriting if it already exists.
"""

import pandas as pd
import numpy as np
import arrow
from pathlib import Path
import random


def get_files(path: str, suffix='csv') -> list:
    """Creates a set of all files of a specific file ending in a folder, including sub-folders.

    Args:
        path (str): The path to look for files in.
        suffix (str, optional): Specify the file ending . Defaults to 'csv'.

    Returns:
        path_list: list of all file-paths in the folder
    """    
    return list(Path(path).rglob(f'*.{suffix}'))


def pick_random_csv(path: str, random_state=42) -> str:
    """Get the path of a random csv file in the specified folder, including sub-folders.

    Args:
        path (str): The path to look for files in.
        random_seed (int, optional): random.seed(). Defaults to 42.

    Returns:
        str: Path of a random csv file
    """
    random.seed(random_state)
    return random.choice(get_files(path))


def save_without_overwrite(data: pd.DataFrame, file_path):
    """Save a pd.DataFrame as csv to a specified path.
       - Makes sure to create the path if it doesn't exist.
       - Does not overwrite existing files.
        

    Args:
        data (pd.DataFrame): Any pandas DataFrame
        file_path (str or Path()): Location can be specified as either a string or any other Path() object.

    Raises:
        FileExistsError: Raises an error if the file already exists and should not be overwritten.
    """
    file_path = Path(file_path)
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True)
    if file_path.is_file():
        raise FileExistsError(f"The file {file_path} already exists.")
    else:
        data.to_csv(file_path, index=True)