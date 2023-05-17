import pandas as pd
import datetime as dt
from typing import Union, List

def set_datetime_index(ts_df: pd.DataFrame, date='date') -> pd.DataFrame:
    """Takes a date-string column as argument, converts it to datetime format and sets it as the new index of the DataFrame.


    Args:
        ts_df (pd.DataFrame): A time-series DataFrame that isn't already in time-series format
        date (str, optional): Name of the column with dates in string format. Defaults to 'date'.

    Returns:
        pd.DataFrame: A sorted time-series DataFrame.
    """
    if date in ts_df.columns:
        ts_df = ts_df.assign(
            date = pd.to_datetime(ts_df[date])
            ) \
        .set_index(date) \
        .sort_index()
            
    return ts_df

def set_panel_index(ts_df: pd.DataFrame, date='date', individual='', names=[]) -> pd.DataFrame:
    """Takes a date-string column and an individual-column as argument and convert the DataFrame in a multi-index panel DataFrame

    Args:
        ts_df (pd.DataFrame): A time-series DataFrame that isn't already in time-series format
        date (str, optional): Name of the column with dates in string format. Defaults to 'date'.
        individual (str, optional): Name of the column with the specified panel individuals. Must not be an empty string.
        names (list, optional): List of names for the indices.

    Raises:
        ValueError: The individual must be specified.

    Returns:
        pd.DataFrame: A multi-index panel DataFrame sorted by the datetime column.
    """
    datetime_df = set_datetime_index(ts_df, date=date)

    if individual:
        datetime_df = datetime_df.set_index(individual, append=True)
    else:
        raise ValueError('individual can not be an empty string')
    
    if names:
        datetime_df.index = datetime_df.index.set_names(names)
 
    return datetime_df


def get_unique_timestamps(df: pd.DataFrame, date='date') -> pd.Series:
    """Assuming a time-series DataFrame has their index set in Datetime format, returns a series with all unique timestamps

    Args:
        df (pd.DataFrame): A DataFrame in time-series format

    Raises:
        TypeError: The index must be a DatetimeIndex

    Returns:
        pd.Series: A pd.Series with unique timestamps of the original DataFrame in Datetime format, indexed with identical DatetimeIndex.
    """
    if isinstance(df.index.get_level_values(date), pd.DatetimeIndex):
        return df.index.get_level_values(date).unique().to_series()
    else:
        raise TypeError('Index is not Datetime')
    
def get_unique_index(df: pd.DataFrame, ind: Union[str, int]) -> pd.Series:
    """Returns a pd.Series of unique indices in the specified index.

    Args:
        df (pd.DataFrame): _description_
        index (Union[str, int]): _description_

    Returns:
        pd.Series: _description_
    """
    return pd.Series(df.index.get_level_values(ind).unique())

    
def search_by_index(df: pd.DataFrame, index: Union[str, int], list_of_indices: List[str]) -> pd.DataFrame:
    pass


def extend_panel(df: pd.DataFrame, date='date', individual='station_uuid', names=['date','station']) -> pd.DataFrame:
    """Calls the methods to convert the DataFrame into a panel with a date and an individual column.
    Then stratifies the DataFrame by extending all timestamps to all stations

    Args:
        df (pd.DataFrame): A DataFrame with at least one 'date' column and a second column representing individuals of a panel.
        date (str, optional): Name of the date column. Defaults to 'date'.
        individual (str, optional): Name of the individual column. Defaults to 'station_uuid'.
        names (list, optional): Names for [date, individual] indices . Defaults to ['date','station'].

    Returns:
        pd.DataFrame: MultiIndex DataFrame with one time-series.
    """
    df = set_panel_index(df, date=date, individual=individual)
    timestamps = get_unique_timestamps(df, date)
    stations = get_unique_index(df, individual)
    new_index = pd.MultiIndex.from_product([timestamps, stations], names=names)
    return df.reindex(new_index)


def swap_sort_index(df: pd.DataFrame) -> pd.DataFrame:
    """Swaps the indices of a 2-index MultiIndex DataFrame and sorts the DataFrame by the new level-1 index.

    Args:
        df (pd.DataFrame): A MultiIndex Dataframe

    Returns:
        pd.DataFrame: A MultiIndex Dataframe with swapped indices, sorted by the new level-1 index.
    """
    return df.swaplevel(0,1).sort_index()


def add_time_columns(df: pd.DataFrame, date='date', attributes=['year', 'month', 'day', 'dayofyear', 'dayofweek', 'hour', 'minute']) -> pd.DataFrame:
    """Takes a Dataframe with a DateTime Index and creates columns for 
    ['year', 'month', 'day', 'dayofyear', 'dayofweek', 'hour', 'minute']

    Args:
        df (pd.DataFrame): DataFrame with DateTime ndex
        date (str, optional): Name of the DateTime index. Defaults to 'date'.
        attributes (list, optional): List if attributes from the DateTime library to create columns from. Defaults to ['year', 'month', 'day', 'dayofyear', 'dayofweek', 'hour', 'minute'].

    Returns:
        pd.DataFrame: DateTime Index DataFrame with new time columns
    """
    timestamps = df.index.get_level_values(date)
    return df.assign(**{attr: getattr(timestamps, attr) for attr in attributes})
