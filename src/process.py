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


def get_unique_timestamps(df: pd.DataFrame) -> pd.Series:
    """Assuming a time-series DataFrame has their index set in Datetime format, returns a series with all unique timestamps

    Args:
        df (pd.DataFrame): A DataFrame in time-series format

    Raises:
        TypeError: The index must be a DatetimeIndex

    Returns:
        pd.Series: A pd.Series with unique timestamps of the original DataFrame in Datetime format, indexed with identical DatetimeIndex.
    """
    if isinstance(df.index, pd.DatetimeIndex):
        return df.index.unique().to_series()
    else:
        raise TypeError('Index is not Datetime')