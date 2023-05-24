'''Various helper functions to get distances between stations'''

import os
import sys
import time
import math
import json
import pandas as pd
import numpy as np
from tqdm import tqdm

from pathlib import Path

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.config import paths

import openrouteservice as ors

# get API-Key for ORS
from dotenv import load_dotenv
load_dotenv()
if not os.getenv("ORS_KEY"):
    raise TypeError("'ORS_KEY' variable not found in .env file")
ORS_KEY = os.getenv("ORS_KEY")

import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

EARTH_RADIUS = 6371  # Radius of the Earth in kilometers


def calc_geo_distance(lat1, lon1, lat2, lon2) -> float:
    '''Calculate geo distance between two points'''

    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return EARTH_RADIUS * c



def check_cols_uuid(df: pd.DataFrame):
    '''
    Check for uuid column, raises exception if not found.
    
    Parameters:
        df (pd.DataFrame): A dataframe to check
    '''

    if not set(["uuid"]).issubset(df.columns):
        raise Exception("We need the uuid column in your dataframe")  
    


def check_cols_latlon(df: pd.DataFrame):
    '''
    Check for latitude, longitude columns, raises exception if not found.
    
    Parameters:
        df (pd.DataFrame): A dataframe to check
    '''

    if not set(["latitude", "longitude"]).issubset(df.columns):
        raise Exception("We need latitude and longitude columns in your dataframe")  


def load_station_file():
    '''Loads dummy stations file for quick start / debugging. '''
    stations_df = pd.read_csv(paths.SAMPLE_DIR / "stations" / "stations_dus_plus.csv")
    check_cols_uuid(stations_df)
    check_cols_latlon(stations_df)

    return stations_df



def save_station_matrix(matrix: pd.DataFrame, filename: str):
    '''
    Saves matrix as CSV into SAMPLE_DIR / stations /
    
    Parameters:
        matrix (pd.DataFrame): The matrix to save
        filename (str):        The filename to save under
    '''
    matrix.to_csv(paths.SAMPLE_DIR / "stations" / filename)



def create_station_matrix(station_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Create an empty matrix from a station dataframe
        
    Parameters:
        station_df (pd.DataFrame): List of stations as dataframe
                                   Must include uuid, latitude, longitude
    Returns:
        station_matrix (pd.DataFrame): An empty NxN matrix of all stations - including lat, lon
    '''
    # extract only basic info for calculation
    check_cols_uuid(station_df)
    check_cols_latlon(station_df)
    station_matrix = station_df[["uuid", "longitude", "latitude"]].copy()

    # build a UUID x UUID matrix to fill up with distances later
    # CAUTION: we may have UUIDs with differing lat, lon info
    #          will drop dups for now - HAVE TO TAKE CARE
    station_matrix.drop_duplicates(subset="uuid", inplace=True)

    # create a 0-filled column for each uuid
    uuid_list = station_matrix["uuid"]

    for c in tqdm(range(0,len(uuid_list)), desc="Adding station columns to matrix"):
        uuid = uuid_list[c]
        station_matrix[uuid] = 0
        
    # for easier station lookups, index is set on the uuid column
    station_matrix.set_index("uuid", inplace=True)

    return station_matrix



def create_distance_matrix(station_matrix: pd.DataFrame) -> pd.DataFrame:
    '''
    Fills a station matrix with geospatial distances (in kilometers) between each station.
    
    Parameters:
        station_matrix (pd.DataFrame): A station matrix to be filled

    Returns:
        filled_matrix (pd.DataFrame): New dataframe containing the filled matrix.
                                      Distances are given in kilometers.
    '''
    # don't touch the original dataframe
    filled_matrix = station_matrix.copy()

    check_cols_latlon(filled_matrix)

    uuid_list = filled_matrix.index

    # calc & insert geo distances for all stations
    for c in tqdm(range(0,len(uuid_list)), desc="Calculating distances for stations"):
        origin = uuid_list[c]

        # lesson learned: don't update matrix cell-by-cell
        #                 (e.g. matrix.loc[origin, destination] = distance)
        #                 this is 4-8 times slower than updating a whole matrix-row
        #                 even without caring for duplicates (geo distance for start-dest is the same as dest-start)
        # insert complete rows
        current_row = [filled_matrix.loc[origin, "longitude"], filled_matrix.loc[origin, "latitude"]]

        for destination in uuid_list:
            origin_lat, origin_lon = filled_matrix[["latitude", "longitude"]].loc[origin]
            dest_lat, dest_lon     = filled_matrix[["latitude", "longitude"]].loc[destination]

            dist = calc_geo_distance(origin_lat, origin_lon, dest_lat, dest_lon)
            current_row.append(dist)
        
        filled_matrix.loc[origin] = current_row
    
    return filled_matrix



def create_duration_matrix(station_matrix: pd.DataFrame) -> pd.DataFrame:
    '''
    Fills a station matrix with driving durations times (in seconds)
    between each station. Uses openrouteservice.org API.
    
    Parameters:
        station_matrix (pd.DataFrame): A station matrix to be filled

    Returns:
        filled_matrix (pd.DataFrame): New dataframe containing the filled matrix.
                                      Durations (driving times) are given in seconds.
    '''
    # don't touch the original dataframe
    filled_matrix = station_matrix.copy()
    check_cols_latlon(filled_matrix)
    uuid_list = filled_matrix.index

    # raise exception if result would be more than 250 API calls (limit is 500 per day)
    # TO DO: request remaining API calls from openrouteservice.org and compare against
    #        API calls that would be made now
    if station_matrix.shape[0] > 250:
        raise Exception(f"Request would generate {station_matrix.shape[0]} API calls. Only 250 API calls allowed per function call.")  

    ors_client = ors.Client(key=ORS_KEY)
    
    lonlat_pairs = filled_matrix[["longitude","latitude"]].to_numpy()
    lonlat_pairs_list = lonlat_pairs.tolist()

    # if number of stations <= 50, do it with one single matrix API call
    # if number of stations > 50, split into one API call per station
    if station_matrix.shape[0] <= 50:
        try:
            ors_response = ors_client.distance_matrix(lonlat_pairs.tolist(),
                                                      profile="driving-car")
        except ors.exceptions.ApiError as err:
            print(f"The ORS API reported the error: {err}")
            print("No durations have been filled into your matrix.")
        else:
            distance_string = json.dumps(ors_response)
            distance_json = json.loads(distance_string)
            # duration matrix comes as list of list, can simply be assigned to df
            filled_matrix.iloc[0:, 2:] = distance_json["durations"]


    else:
        for c in tqdm(range(len(lonlat_pairs_list)), desc="Getting durations for stations"):
            time.sleep(2)   # stay below API limit (40 calls/minute)
            try:
                ors_response = ors_client.distance_matrix(lonlat_pairs_list,
                                                            profile = "driving-car",
                                                            sources = [c],
                                                            metrics = ['duration'])
            except ors.exceptions.ApiError as err:
                print(f"The ORS API reported the error: {err}")
                print("No durations have been filled into your matrix.")    
            else:
                distance_string = json.dumps(ors_response)
                distance_json = json.loads(distance_string)
                duration_list = distance_json["durations"]
                filled_matrix.iloc[c, 2:] = duration_list[0]

    return filled_matrix
