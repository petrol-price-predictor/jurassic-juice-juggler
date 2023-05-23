import pandas as pd
import numpy as np
import inspect

from . import process
from .config.paths import SAMPLE_DIR

def process_data(data: pd.DataFrame, last_closing_prices: pd.DataFrame):

    # Drop the 'change' columns for now as they dont provide us with any insight. FUTURE FEATURE ENGINEERING
    data = data.drop(columns=data.filter(like='change').columns)

    # Stratify the panel by cross-multiplying all timestamps with all stations and set a MultiIndex
    data = process.extend_panel(data)
    data = process.swap_sort_index(data)

    # If the first row is empty, impute them with the closing prices from the previous day
    # SOMETIMES DOESNT WORK, FIX
    if not last_closing_prices.empty:
        data = impute_closing_prices(data, last_closing_prices)

    # ForwardFill all prices until a price-change occurs
    data = fill_missing_prices(data)

    return data

def merge_sort_index(data):
        return pd.concat(data, ignore_index=True).sort_values(['station', 'date'])

def get_metadata(data: pd.DataFrame):
    return pd.DataFrame([{
        "date": data.tail(1).index.get_level_values(1)[0].date(),
        "diesel_mean": data.diesel.mean().round(3),
        "e5_mean": data.e5.mean().round(3),
        "e10_mean": data.e10.mean().round(3),
        }])


def get_closing_prices(prices_df):
    closing_prices = prices_df.groupby(level='station').tail(1).reset_index(level='date')
    return closing_prices


def impute_closing_prices(new_prices: pd.DataFrame, closing_prices: pd.DataFrame):

    opening_prices = new_prices.groupby(level='station').head(1).reset_index(level=1)
    opening_prices = opening_prices.fillna(closing_prices)

    # set the datetime index back to where it was and update the new prices with the opening prices
    opening_prices = opening_prices.set_index('date', append=True)
    new_prices.update(opening_prices, overwrite = False)
    return new_prices


def fill_missing_prices(prices_df: pd.DataFrame):
    prices_df[['diesel', 'e5', 'e10']] = prices_df \
        .groupby(level='station')[['diesel', 'e5', 'e10']] \
        .fillna(method='ffill') \
        .fillna(method='bfill')
    prices_df[['diesel', 'e5', 'e10']] = prices_df.replace(0, np.nan)
    prices_df = prices_df.assign(
        diesel_is_selling = prices_df['diesel'].apply(lambda x: 0 if pd.isna(x) else 1),
        e5_is_selling = prices_df['e5'].apply(lambda x: 0 if pd.isna(x) else 1),
        e10_is_selling = prices_df['e10'].apply(lambda x: 0 if pd.isna(x) else 1),
    )
    prices_df[['diesel', 'e5', 'e10']] = prices_df \
        .groupby(level='station')[['diesel', 'e5', 'e10']] \
        .fillna(method='ffill') \
        .fillna(method='bfill')
   
    return prices_df

def split_panel(prices_df: pd.DataFrame, split):
    prices_df = process.set_panel_index(prices_df, date='date', individual='station')
    split_data = {name: prices_df[prices_df.filter(like=name).columns] for name in split}
    return split_data

def make_hourly(data: pd.DataFrame)->pd.DataFrame:
    data = process.set_panel_index(data, date='date', individual='station')
    return data

def get_methods()->dict:
    return {
            'resample_timestamps': resample_timestamps
        }


def resample_timestamps(prices_df: pd.DataFrame, agg_dict: dict, freq: str = 'H')->pd.DataFrame:
    
    # need to call this function first otherwise pd.to_datetime will bug without raising and error but can't transform days with shift in daylight saving time
    prices_df = process.set_panel_index(prices_df, date='date', individual='station')

    #extracting the minute information before removing it from the DateTime index
    prices_df['total_changes'] = prices_df.index.get_level_values('date').minute

    # unfortunately MultiIndex DFs dont allow for direct transformation, so an ugly workaround is required to floor the DateTime index
    prices_df = prices_df.reset_index(level='date')
    prices_df['date'] = prices_df['date'].dt.floor(freq)
    prices_df = prices_df.set_index('date', append=True).sort_index()

    # grouping by station and freq-bins, using specified aggregation for all columns. groupby is agnostic to DataTypes so not all aggregations work on every DataType
    prices_df = prices_df.groupby(['station','date']).agg(agg_dict)

    # creating a new index-object that serves as a mask for the resampled DataFrame with equidistant timestamps
    stations = prices_df.index.get_level_values('station').unique()
    min_date = prices_df.index.get_level_values('date').min().floor('D')
    max_date = prices_df.index.get_level_values('date').max().ceil('D') - pd.Timedelta(1, unit='us')
    date_range = pd.date_range(min_date, max_date, freq=freq)
    resampled_index = pd.MultiIndex.from_product([stations, date_range], names=['station', 'date'])

    # applying the resampled index to the original DataFrame and filling the NaNs.
    prices_df = prices_df.reindex(resampled_index).ffill().bfill()

    return prices_df

if __name__ == "__main__":
    pass
