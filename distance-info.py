#
# collection various distance infos for each station
#

import glob
import re
import math
import json
import pandas as pd
import numpy as np
import plotly.express as px
from tqdm import tqdm

# Joshuas nice little helper function
R = 6371  # Radius of the Earth in kilometers

def get_distance(lat1, lon1, lat2, lon2):
    #R = 6371  # Radius of the Earth in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c




# 1. collect all stations from a given time period (start with 1 week) with master data:
# columns to take over:
#    - uuid,name,brand,street,house_number,post_code,city,latitude,longitude
# columns to create
#   - first_seen: date where station occured first
#   - last_seen: date where station occured last

work_dir = "data/stations/2023/05"
station_files = glob.glob(work_dir + "/*.csv")
station_list = pd.DataFrame()

for filename in station_files:
    current_file = pd.read_csv(filename)
    current_file["file_date"] = re.search("[\d]{4}-[\d]{2}-[\d]{2}",filename).group()
    station_list = pd.concat([station_list, current_file])

station_list["file_date"] = pd.to_datetime(station_list["file_date"])

# extract only basic info for calculation
station_location = station_list[["uuid", "longitude", "latitude"]].copy()

# build a UUID x UUID matrix to fill up with distances later

station_matrix = station_location.copy()

# CAUTION: we may have UUIDs with differing lat, lon info
#          will drop dups for now - HAVE TO TAKE CARE
station_matrix.drop_duplicates(subset="uuid" ,inplace=True)
uuid_list = station_matrix["uuid"]

for uuid in uuid_list:
    station_matrix[uuid] = 0

station_matrix.set_index("uuid", inplace=True)
station_distances = station_matrix.copy()
#station_distances = station_matrix

# calculate distances for a single test_station
# to test just do it until c == 100
#c = 0

for c in tqdm(range(0,len(uuid_list)), desc="Stations"):
    #c += 1
    #print("Station #" + str(c))
    #if c == 10: break
    
    start = uuid_list[c]
    for destination in uuid_list:
        start_lat = station_distances.loc[start, "latitude"]
        start_lon = station_distances.loc[start, "longitude"]
        dest_lat = station_distances.loc[destination, "latitude"]
        dest_lon = station_distances.loc[destination, "longitude"]

        if station_distances.loc[start, destination] == 0:
            distance = get_distance(start_lat, start_lon, dest_lat, dest_lon)
            station_distances.loc[start, destination] = distance
            station_distances.loc[destination, start] = distance

station_distances.to_csv("stations_test.csv")