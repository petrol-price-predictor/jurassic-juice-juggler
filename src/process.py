import pandas as pd
import datetime as dt

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


def get_unique_timestamps(df: pd.DataFrame) -> pd.Series:
    """Assuming a time-series DataFrame has their index set in Datetime format, returns a series with all unique timestamps

    Args:
        df (pd.DataFrame): A DataFrame in time-series format

    Raises:
        TypeError: The index must be a DatetimeIndex

    Returns:
        pd.Series: A pd.Series with unique timestamps of the original DataFrame in Datetime format, indexed with identical DatetimeIndex.
    """
    if isinstance(df.index.get_level_values(0), pd.DatetimeIndex):
        return df.index.get_level_values(0).unique().to_series()
    else:
        raise TypeError('Index is not Datetime')