Scrapper ETL reads ads from fotocasa, extracts information from each one, and uploads them to a local database.
CleanHousingData ETL cleans scrapper data, adds geo info, and uploads it a different table in database.
PriceModel ETL constructs, fits, and tunes a model for price prediction, saves the models as pkl files locally, performs prediction on the values in database and uploads them to a new table.

Analysis contains notebooks that extract the data and performs different analysis.