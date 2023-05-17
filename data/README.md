# Jurassic-Juice-Juggler
Predicting petrol prices based on live data from German petrol stations

---

## Overview
- This README gives a description of the raw data files from _tankerkönig.de_.

- For background information on how stations change their prices and the structure of the petrol station market in Germany, see [Data generation & further processing](data-generation-processing.md).

---
# Prices
Fields in the CSV Header:

`date,station_uuid,diesel,e5,e10,dieselchange,e5change,e10change`

Meaning of the fields:

|Field        |Meaning                                     |
|------------|--------------------------------------------|
|date        |Time of change                              |
|station_uuid|UUID of the petrol station from `stations`  |
|diesel      |Price Diesel                                |
|e5          |Price Super E5                              |
|e10         |Price Super E10                             |
|dieselchange|0=no change, 1=change, 2=removed, 3=new     |
|e5change    |0=no change, 1=change, 2=removed, 3=new     |
|e10change   |0=no change, 1=change, 2=removed, 3=new     |


# Petrol Stations
In the _stations_ directory, all petrol stations are listed in a CSV file.
As the list of petrol stations changes, it is exported daily to a directory stations/YEAR/MONTH/

Fields in the CSV Header:

`uuid,name,brand,street,house_number,post_code,city,latitude,longitude`

Meaning:

|Field             |Meaning                                      |
|-----------------|-----------------------------------------------|
|uuid             |UUID of the petrol station, matches with the prices|
|name             |Petrol station name                           |
|brand            |Brand                                         |
|street           |Street                                        |
|post_code        |Postal code                                   |
|city             |City                                          |
|latitude         |Geographical latitude                         |
|longitude        |Geographical longitude                        |
|first_active     |First appearance                              |
|openingtimes_json|Opening hours as JSON                         |

# openingtimes_json
applicable_days: the days when these opening hours apply, binary encoded - one byte.

|Value            |Meaning                                      |
|-----------------|-----------------------------------------------|
|1|Monday|
|2|Tuesday|
|4|Wednesday|
|8|Thursday|
|16|Friday|
|32|Saturday|
|64|Sunday|
|128|Holiday|

Example:
* Monday - Friday = 31 (1 + 2 + 4 + 8 + 16)
* Sat/Sun = 96 (32 + 64)
* Holidays = 128

# Allocation
Each petrol station is uniquely identified by a UUID. This UUID is referenced in the price data.

# License of the data collection
<https://creativecommons.org/licenses/by-nc-sa/4.0/>
For commercial use, we provide the data under a different license. Inquiries at <info@tankerkoenig.de>

