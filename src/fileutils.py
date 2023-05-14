import pandas as pd
import numpy as np
import arrow
from pathlib import Path
import random


def get_files(path: str, suffix='csv') -> list:
    """Creates a list of all files of a specific file ending in a folder, including sub-folders.

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