# jurassic-juice-juggler
predicting petrol prices based on german petrol station live data

Felder im CSV-Header:

`date,station_uuid,diesel,e5,e10,dieselchange,e5change,e10change`

Bedeutung der Felder:

|Feld        |Bedeutung                                      |
|------------|-----------------------------------------------|
|date        |Änderungszeitpunkt                             |
|station_uuid|UUID der Tankstelle aus `stations`             |
|diesel      |Preis Diesel                                   |
|e5          |Preis Super E5                                 |
|e10         |Preis Super E10                                |
|dieselchange|0=keine Änderung, 1=Änderung, 2=Entfernt, 3=Neu|
|e5change    |0=keine Änderung, 1=Änderung, 2=Entfernt, 3=Neu|
|e10change   |0=keine Änderung, 1=Änderung, 2=Entfernt, 3=Neu|


# Tankstellen
Im Verzeichnis _stations_ sind alle Tankstellen in einer CSV-Datei aufgeführt. 
Da sich die Tankstellenliste ändert, wird sie täglich in ein Verzeichnis stations/JAHR/MONAT/ exportiert

Felder im CSV-Header:

`uuid,name,brand,street,house_number,post_code,city,latitude,longitude`

Bedeutung:

|Feld             |Bedeutung                                      |
|-----------------|-----------------------------------------------|
|uuid             |UUID der Tankstelle, matcht mit den Preisen    |
|name             |Tankstellenname                                |
|brand            |Marke                                          |
|street           |Straße                                         |
|post_code        |Postleitzahl                                   |
|city             |Stadt                                          |
|latitude         |geogr. Breite                                  |
|longitude        |geogr. Länge                                   |
|first_active     |erstes Auftauchen                              |
|openingtimes_json|Öffnungszeiten als JSON                        |

# openingtimes_json
applicable_days: die Tage, an denen diese Öffnungszeit gültig ist, binär kodiert - ein Byte.

|Wert             |Bedeutung                                      |
|-----------------|-----------------------------------------------|
|1|Montag|
|2|Dienstag|
|4|Mittwoch|
|8|Donnerstag|
|16|Freitag|
|32|Samstag|
|64|Sonntag|
|128|Feiertag|

Bsp:
* Montag - Freitag = 31 (1 + 2 + 4 + 8 + 16)
* Sa/So = 96 (32 + 64)
* Feiertags = 128

# Zuordnung
Jede Tankstelle ist eindeutig über eine UUID identifiziert. In den Preisdaten wird diese UUID referenziert.

# Lizenz der Datensammlung
<https://creativecommons.org/licenses/by-nc-sa/4.0/>
Für kommerzielle Nutzung stellen wir die Daten unter anderer Lizenz zur Verfügung. Nachfrage unter <info@tankerkoenig.de>
