import pandas as pd
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
    if not last_closing_prices.empty:
        data = impute_closing_prices(data, last_closing_prices)

    # ForwardFill all prices until a price-change occurs
    data = fill_missing_prices(data)

    return data

def get_metadata(data: pd.DataFrame):
    return pd.DataFrame([{
        "date": data.tail(1).index.get_level_values(1)[0].date(),
        "diesel_mean": data.diesel.mean().round(3),
        "e5_mean": data.e5.mean().round(3),
        "e10_mean": data.e10.mean().round(3),
        }])


def get_closing_prices(prices_df):
    return prices_df.groupby(level='station').tail(1)


def impute_closing_prices(new_prices: pd.DataFrame, closing_prices: pd.DataFrame):

    opening_prices = new_prices.groupby(level='station').head(1).reset_index(level=1)
    opening_prices = opening_prices.fillna(closing_prices.reset_index(level=1))

    # set the datetime index back to where it was and update the new prices with the opening prices
    opening_prices = opening_prices.set_index('date', append=True)
    new_prices.update(opening_prices, overwrite = False)
    return new_prices


def fill_missing_prices(prices_df: pd.DataFrame, method='ffill'):
    prices_df[['diesel', 'e5', 'e10']] = prices_df \
        .groupby(level='station')[['diesel', 'e5', 'e10']] \
        .fillna(method=method)
    
    return prices_df

def make_hourly(data: pd.DataFrame)->pd.DataFrame:
    data = data.set_index('datetime').sort_index()
    return data

def get_methods()->dict:
    return {
            'hourly': make_hourly
        }

if __name__ == "__main__":
    pass
