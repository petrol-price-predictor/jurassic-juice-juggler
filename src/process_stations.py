from . import process
from geopy.geocoders import Nominatim
from holidays import Germany
import pandas as pd

german_state_abbreviations = {
    'Baden-Württemberg': 'BW',
    'Bayern': 'BY',
    'Berlin': 'BE',
    'Brandenburg': 'BB',
    'Bremen': 'HB',
    'Hamburg': 'HH',
    'Hessen': 'HE',
    'Mecklenburg-Vorpommern': 'MV',
    'Niedersachsen': 'NI',
    'Nordrhein-Westfalen': 'NW',
    'Rheinland-Pfalz': 'RP',
    'Saarland': 'SL',
    'Sachsen': 'SN',
    'Sachsen-Anhalt': 'ST',
    'Schleswig-Holstein': 'SH',
    'Thüringen': 'TH'
}

def get_state_by_coordinates(lat, long):
    geolocator = Nominatim(user_agent='geoapiExercises')
    location = geolocator.reverse((lat, long), exactly_one=True)
    address = location.raw['address']
    state = address.get('state', '')
    return state


def get_location_state_by_uuid(stations_data, uuid):
    station_row = stations_data[stations_data['uuid'] == uuid]
    if not station_row.empty:
        latitude = station_row['latitude'].values[0]
        longitude = station_row['longitude'].values[0]
        state = get_state_by_coordinates(latitude, longitude)
        return (state, german_state_abbreviations[state])
    else:
        raise ValueError(f'No station with found with UUID: {uuid}')
    
    
def get_german_holidays(data, state, date_column='date'):

    if not (
        issubclass(data[date_column].dtype.type, pd.core.dtypes.dtypes.DatetimeTZDtype) 
        or issubclass(data[date_column].dtype.type, pd._libs.tslibs.timestamps.Timestamp)
    ):
        raise ValueError("date_column must be datetime")
        
    # Get the minimum and maximum date (start date and end date)
    start_date_h = data[date_column].min().date()
    end_date_h = data[date_column].max().date()

    # Retrieve German holidays within the specified date range
    german_holidays = Germany(years=range(start_date_h.year, end_date_h.year + 1), prov=state)

    # Create a new 'IsHoliday' column in the DataFrame
    data['is_holiday'] = 0

    # Set 'IsHoliday' to 1 if the date part is a holiday
    data.loc[data[date_column].dt.date.isin(german_holidays.keys()), 'is_holiday'] = 1

    return data
        
    

def process_csv(file):
    pass
    # Code to process a price file goes here.

def collect_metadata(data):
    pass
    # function that returns a datastructure to add metadata to the metadata DataFrame

if __name__ == "__main__":
    pass
