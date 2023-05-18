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


def save_closing_prices(df, file_path, date='date'):
    """Saves the closing prices of the day to a csv file.
    Checks for file availability and 

    Args:
        df (_type_): _description_
        file_path (_type_): _description_
        date (str, optional): _description_. Defaults to 'date'.
    """
    file_path = Path(file_path)
    
    # If the file doesn't exist, write the DataFrame to a new CSV file
    if not file_path.is_file():        
        df.to_csv(file_path, index=True)

    # If it does exist, compare the last line of the CSV File with the last line of the DataFrame df
    else:
        with open(file_path, "r") as file:
            last_line = deque(file, 1)[0]

        # Making sure the lines format is comparable 
        # CURRENTLY ONLY WORKS WITH DATE ON COLUMN INDEX 1
        old_timestamp = pd.to_datetime(last_line.split(',')[1])
        new_timestamp = pd.to_datetime(df[date].max())
        
        # If the new data is not already in the CSV File, append the DataFrame and safe the CSV file.
        if new_timestamp <= old_timestamp:
            print("Some data already exists in the CSV file. Data was not appended.")
        else:
            df.to_csv(file_path, mode='a', header=False, index=True)