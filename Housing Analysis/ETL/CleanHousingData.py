import boto3
import psycopg2
import pandas as pd
import googlemaps


def execute_query(q):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    db_pass = ssm.get_parameter(Name="POSTGRESQL_PASS", WithDecryption=True)
    db_pass = db_pass["Parameter"]["Value"]

    connection = psycopg2.connect(database="Housing",
                                              host="localhost",
                                              user="postgres",
                                              password=db_pass,
                                              port=5432)

    try:
        df = pd.read_sql(q, connection)
        return df
    except Exception as e:
        raise e
    finally:
        connection.close()


def upload_to_table(elements, table_name):
    pass


# cols_scrapper = ['id', 'url', 'timestamp', 'price', 'rooms', 'bathrooms', 'surface',
#        'street_name', 'city', 'type', 'orientation', 'age', 'parking', 'floor',
#        'elevator', 'furniture', 'active', 'state', 'energy', 'emissions',
#        'pets', 'heating', 'water_heating', 'full_street_city']

# cols_clean = ['id', 'timestamp', 'price', 'rooms', 'bathrooms', 'surface',
#        'city', 'type', 'orientation', 'age', 'parking', 'floor',
#        'elevator', 'furniture', 'state', 'energy', 'emissions',
#        'pets', 'heating', 'water_heating', 'formatted_address', 'latitude', 'longitude']

# new_cols = ["lat", "lng", "formatted_address"]



q = """
SELECT hs.id
    , hs.timestamp
    , hs.price
    , hs.rooms
    , hs.bathrooms
    , hs.surface
    , hs.city
    , hs.type
    , hs.orientation
    , hs.age
    , hs.parking
    , hs.floor
    , hs.elevator
    , hs.furniture
    , hs.state
    , hs.energy
    , hs.emissions
    , hs.pets
    , hs.heating
    , hs.water_heating
    , hs.full_street_city || ', ' || hs.city AS full_street_city
    
FROM houses_scrapper hs
LEFT JOIN houses_clean hc
ON hs.id = hc.id

WHERE hc.id IS NULL
"""
df = execute_query(q)


# Filter prices and surface (<=400)
# df = df.loc[df["prices"] <= 10000, :]
#  ('energy', 'emissions'): replace Exento to 0
    # df[c] = df[c].str.replace("Exento", "0").astype(float)
# replace none with null or nan in cols_cat
# for c in cols_cat:
    # df[c] = df[c].fillna("NULL")


maps_key = "AIzaSyD8V5kRghqeYicSYTeW6SnFEbM1Cx8_9yw"

# WARNING!! Limit to 40k request monthly. Around 4k are in db for all pages in a clean run
gmaps = googlemaps.Client(key=maps_key)

address = "Sant Joan , Dreta de l'Eixample, Barcelona Capital, ES"
# Geocoding an address
geocode_result = gmaps.geocode(address)

# geocode_result[0]["geometry"]["location"] lat/lng
# geocode_result[0]["formatted_address"]

# update timestamp