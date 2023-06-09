

#In this version, extract_opening_times returns a dictionary of dictionaries with keys
# '0' through '7', where each inner dictionary has keys 'start' and 'end' for the start
# and end times of the opening period on that day. The apply function then converts this
# dictionary into a dataframe with columns for each day and separate columns for start and
# end times. Finally, the new dataframe is concatenated with the original dataframe, and the
# 'openingtimes_json' column is dropped. This code uses the pd.to_datetime() function to convert
# the specified columns to datetime type within the opening_times_df DataFrame.

import pandas as pd
import json

def get_days(day_code):
    """ 
    Function Description:
The function get_days(day_code) takes a day_code as an argument and returns a list of corresponding
days based on the binary representation of the day_code. It uses a dictionary to map specific day_code
values to their corresponding days of the week.

Arguments:
The function get_days() accepts one argument:
day_code: An integer representing the day code. It is used to determine which days are included in the returned list.

Return Value:
The function returns a list of days. Each element in the list represents a day of the week corresponding to the set
bits in the day_code. The days are represented as strings.
For example, if day_code is 6 (binary: 110), the function would return ["1", "2"], indicating that the second
and third days of the week are included (assuming Monday is considered the first day).
    """
    
    days_dict = {
        1: "0",
        2: "1",
        4: "2",
        8: "3",
        16: "4",
        32: "5",
        64: "6",
        128: "7"
    }
    days = []
    for code, day in days_dict.items():
        if day_code & code:
            days.append(day)
    return days


def extract_opening_times(row, date):
    
    
    """
    Function Description:
The function extract_opening_times is designed to extract opening times from a JSON object
stored in a DataFrame row. It processes the JSON data, converts the opening times to the desired 
format, and returns them as a dictionary.

Arguments:
The function takes two arguments:
row: A row from a DataFrame that contains the opening times JSON object.
date: The date for which the opening times are being extracted.

Return Value:
The function returns the extracted opening times as a dictionary. The dictionary has keys representing
the days of the week (0-7, where 0 corresponds to Sunday) and values containing the start and end times
for each day. The start and end times are represented as datetime objects with the timezone set to UTC+02:00.
    """
    
    opening_times_json = json.loads(row['openingtimes_json'])
    opening_times = {
        '0': {'start': '', 'end': ''},
        '1': {'start': '', 'end': ''},
        '2': {'start': '', 'end': ''},
        '3': {'start': '', 'end': ''},
        '4': {'start': '', 'end': ''},
        '5': {'start': '', 'end': ''},
        '6': {'start': '', 'end': ''},
        '7': {'start': '', 'end': ''}
    }
    if 'openingTimes' in opening_times_json:
        for period in opening_times_json['openingTimes']:
            days = get_days(period['applicable_days'])
            for day in days:
                start_time = pd.Timestamp(period['periods'][0]['startp']) 
                end_time = pd.Timestamp(period['periods'][0]['endp']) 
                if end_time.hour == 24:
                    end_time = end_time.replace(hour=23, minute=59, second=59)

                # add timezone to start and end times
                start_time = start_time.tz_localize('UTC+02:00')
                end_time = end_time.tz_localize('UTC+02:00')

                
                
                opening_times[day]['start'] = start_time
                opening_times[day]['end'] = end_time
    opening_times_df = pd.DataFrame.from_dict(opening_times, orient='index')
    opening_times_df.columns = [f'start_{i}', f'end_{i}']
    opening_times_df = opening_times_df.astype('datetime64[ns, UTC+02:00]')
    
    

# repeat for other columns as needed


    return opening_times


# Specify the path to the folder where the file is located
folder_path = 'data/sample/stations/'

# Read the file and extract the date from the filename
filename = '2023-05-10-stations.csv'
full_path = folder_path + filename
date = filename[:10]

# Read the file into a dataframe
stations_data = pd.read_csv(full_path)



# Apply the function to each row of the dataframe
opening_times_df = stations_data.apply(lambda row: pd.Series(extract_opening_times(row, date)), axis=1)

# Convert dictionary into dataframe with separate columns for start and end times
opening_times_df = pd.concat([
    opening_times_df['0'].apply(pd.Series).add_suffix('_0'),
    opening_times_df['1'].apply(pd.Series).add_suffix('_1'),
    opening_times_df['2'].apply(pd.Series).add_suffix('_2'),
    opening_times_df['3'].apply(pd.Series).add_suffix('_3'),
    opening_times_df['4'].apply(pd.Series).add_suffix('_4'),
    opening_times_df['5'].apply(pd.Series).add_suffix('_5'),
    opening_times_df['6'].apply(pd.Series).add_suffix('_6'),
    opening_times_df['7'].apply(pd.Series).add_suffix('_7')
], axis=1)

opening_times_df = opening_times_df.assign(
    start_0=pd.to_datetime(opening_times_df['start_0']),
    end_0=pd.to_datetime(opening_times_df['end_0']),
    start_1=pd.to_datetime(opening_times_df['start_1']),
    end_1=pd.to_datetime(opening_times_df['end_1']),
    start_2=pd.to_datetime(opening_times_df['start_2']),
    end_2=pd.to_datetime(opening_times_df['end_2']),
    start_3=pd.to_datetime(opening_times_df['start_3']),
    end_3=pd.to_datetime(opening_times_df['end_3']),
    start_4=pd.to_datetime(opening_times_df['start_4']),
    end_4=pd.to_datetime(opening_times_df['end_4']),
    start_5=pd.to_datetime(opening_times_df['start_5']),
    end_5=pd.to_datetime(opening_times_df['end_5']),
    start_6=pd.to_datetime(opening_times_df['start_6']),
    end_6=pd.to_datetime(opening_times_df['end_6']),
    start_7=pd.to_datetime(opening_times_df['start_7']),
    end_7=pd.to_datetime(opening_times_df['end_7'])
)




# Add columns to original dataframe
stations_data = pd.concat([stations_data, opening_times_df], axis=1)

stations_data.drop(['openingtimes_json'], axis=1, inplace=True)

# Save the desired columns in a separate DataFrame
output_df = stations_data[['uuid', 'start_0', 'end_0', 'start_1', 'end_1', 'start_2', 'end_2', 'start_3', 'end_3', 'start_4', 'end_4', 'start_5', 'end_5', 'start_6', 'end_6', 'start_7', 'end_7']]

# Save the DataFrame to a new file
output_path = 'data/sample/stations_time_working.csv'
output_df.to_csv(output_path, index=False)




#*******************************************************************************************************


#In this modified code, the DataFrame df is initialized with the specified state abbreviations as columns.
# For each year and state, the code checks if a holiday is present on a specific date and sets the corresponding
# cell in the DataFrame to True. Finally, any missing values are filled with False to indicate that those dates are
# not holidays.

import requests
import pandas as pd

german_state_abbreviations = [
    'BW',  # Baden-Württemberg
    'BY',  # Bayern
    'BE',  # Berlin
    'BB',  # Brandenburg
    'HB',  # Bremen
    'HH',  # Hamburg
    'HE',  # Hessen
    'MV',  # Mecklenburg-Vorpommern
    'NI',  # Niedersachsen
    'NW',  # Nordrhein-Westfalen
    'RP',  # Rheinland-Pfalz
    'SL',  # Saarland
    'SN',  # Sachsen
    'ST',  # Sachsen-Anhalt
    'SH',  # Schleswig-Holstein
    'TH'   # Thüringen
]

holidays = [
    'Neujahrstag',
    'Heilige Drei Könige',
    'Karfreitag',
    'Ostermontag',
    'TagderArbeit',
    'ChristiHimmelfahrt',
    'Pfingstmontag',
    'Fronleichnam',
    'TagderDeutschen Einheit',
    'Allerheiligen',
    '1.Weihnachtstag',
    '2.Weihnachtstag'
]

df = pd.DataFrame(columns=german_state_abbreviations, dtype=bool)

for year in range(2014, pd.Timestamp.now().year + 1):
    for state in german_state_abbreviations:
        parameters = {
            'jahr': year,
            'nur_land': state,
            'nur_gesetzliche': 1
        }
        response = requests.get(url, params=parameters)
        data = response.json()
        holidays_list = [pd.to_datetime(holiday['datum'], format='%Y-%m-%d').strftime('%d.%m.%Y') for holiday in data.values()]
        
        for holiday in holidays_list:
            df.loc[holiday, state] = True

df = df.fillna(False)
df.index = pd.to_datetime(df.index, format='%d.%m.%Y')


df.to_csv('public_holiday_data.csv', index_label='Date')



#*******************************************************************************************************
