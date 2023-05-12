# The Data generation processing & further processing

## About the petrol station market in Germany
---
- There are about 15.000 petrol stations in Germany as of 2022.
- The absolute number of stations is decreasing in the last 10 years.
- There is still a concentration process going on: corporations are buying smaller chains and independents. Sometimes they close their recently bought stations to open new (usually bigger ones).

### Corporations & Brands

Corporations are said to control their prices for all their stations centrally. They represent **61 % of the petrol stations in Germany**.

|Corporation    | Brands            |Station Share |
|---------------|-------------------|------|
| BP            | Aral, BP          | 16 % |
| Shell plc     | Shell             | 14 % | 
| TotalEnergies | Total, ELAN, FINA | 8 %  |
| ExxonMobil    | Esso              | 7 %  |
| Phillips 66   | Jet               | 6 %  |
| PKN Orlen     | Orlen, Star       | 4 %  |
| Eni           | Agip              | 3 %  |
| Tamoil        | HEM               | 3 %  |


### Independent Brands
These brands are more like interest groups that do buying and marketing together, but are not centrally pushing price changes to the stations.

The biggest independent station are visible as as _AVIA_ and _bft_ 

| Independent | Station Share |
|-------------|-------|
| bft         | 7 %   |
| AVIA        | 6 %   |

The remaining 26 % are smaller operators and single independent petrol stations.

## Data generation process
--- 
- Corporations and smaller chains are said to **control their prices centrally** for all their stations.
- Various journalistic articles reproduce the view that **Aral and Shell are usually the first to start a new round of prices changes**. The other operators are following them.
- It's only for the really small operators and the independents where the station is owned by the same people who operate it. There, a decision to change the price is made only for a single petrol station.


## Data processing
---
- **Petrol stations which sold less than 750.000 liters in the last 12 months (all fuel types combined) do not need to report their prices to the MTS-K.**

- For most of the stations, it seems that it's the petrol station software that pushes prices changes automatically to the MTS-K

- Keep in mind: we pull data provided by tankerkönig.de - not necessarily a 1:1 copy of the original MTS-K data


### Open Questions
- How to correctly interpret the `...change` columns in the `*prices.csv` - especially the `3` value? This is probably tankerkönig-specific.
- What kind of processing / cleaning happens to the data before tankerkönig.de provides it through it's API and repo?




## Useful Links
---
- [Jah­res­be­richt der Markt­trans­pa­renz­stel­le für Kraft­stof­fe 2021](https://www.bundeskartellamt.de/SharedDocs/Publikation/DE/Berichte/Jahresbericht_MTS-K_2021.pdf?__blob=publicationFile&v=6)
German only :( - based on data of the MTS-K for 2021, some basic descriptive analysis and charts. **Hints on price information for crude oil**.
