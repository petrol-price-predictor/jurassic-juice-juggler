import pandas as pd

from . import process
from .config.paths import SAMPLE_DIR

def process_csv(file): #closing_prices

    # if not closing_prices.empty:
    #     # find a way to attach it before the file
    #     pass
        
    prices_df_raw = pd.read_csv(file)
    dus_stations = pd.read_csv(SAMPLE_DIR / 'stations' / 'stations_dus_plus.csv')

    # Create a set of all UUIDs in the DUS subsample
    dus_station_uuid = set(dus_stations.uuid)

    # Drop the 'change' columns for now as they dont provide us with any insight. FUTURE FEATURE ENGINEERING
    prices_df = prices_df_raw.drop(columns=prices_df_raw.filter(like='change').columns)
    prices_df = prices_df[prices_df.station_uuid.isin(dus_station_uuid)]

    df = process.extend_panel(prices_df)
    df = process.swap_sort_index(df)

    # IF FIRST ROW EMPTY, USE PRICE FROM PREVIOUS DAY 'CLOSING_PRICES.CSV'

    df[['diesel', 'e5', 'e10']] = df.groupby(level='station')[['diesel', 'e5', 'e10']].fillna(method='ffill')

    return df

def collect_metadata(data):
    pass
    # function that returns a datastructure to add metadata to the metadata DataFrame

def get_closing_prices(data):
    pass
    # function that returns the last price entry for each station that day

if __name__ == "__main__":
    pass
