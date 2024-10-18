import datetime
import logging

import boto3
import googlemaps
from sqlalchemy import create_engine

from common import execute_query, execute_update_query

_max_price = 10000
_max_surface = 700

logging.basicConfig(level=logging.INFO)


def upload_to_table(df, table_name):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    db_pass = ssm.get_parameter(Name="POSTGRESQL_PASS", WithDecryption=True)
    db_pass = db_pass["Parameter"]["Value"]

    engine = create_engine(f"postgresql://postgres:{db_pass}@localhost:5432/Housing")

    logging.info(f"Uploading {df.shape[0]} elements to {table_name}")
    df["timestamp"] = str(datetime.datetime.now(datetime.timezone.utc))
    df.to_sql(name=table_name, con=engine, index=False, if_exists="append")

    return df


def extract_scrapper_data():
    q = """
    SELECT hs.id
        , hs.timestamp
        , hs.active
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
        , hs.full_street_city || ', ' || hs.city || ', Spain' AS full_street_city

    FROM houses_scrapper hs
    LEFT JOIN houses_clean hc
    ON hs.id = hc.id

    WHERE hc.id IS NULL
    """
    df = execute_query(q)

    return df


def filter_df(df):
    logging.info("Filtering df")
    df = df.loc[df["price"] <= _max_price]
    df = df.loc[df["surface"] <= _max_surface]
    df.loc[df["surface"] <= 1, "surface"] = None
    for c in ("energy", "emissions"):
        df[c] = df[c].str.replace("Exento", "0").astype(float)

    return df


def add_geo_info(df):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    maps_key = ssm.get_parameter(Name="GOOGLE_MAPS_GEOCODING_API_KEY", WithDecryption=True)
    maps_key = maps_key["Parameter"]["Value"]

    # WARNING!! Limit to 40k request monthly. Around 4k are in db for all pages in a clean run
    logging.info("Adding geographic info")
    gmaps = googlemaps.Client(key=maps_key)

    def apply_geoinfo(x):
        address = x["full_street_city"]
        geocode_result = gmaps.geocode(address)
        if geocode_result:
            x["formatted_address"] = geocode_result[0]["formatted_address"]
            x["latitude"] = geocode_result[0]["geometry"]["location"]["lat"]
            x["longitude"] = geocode_result[0]["geometry"]["location"]["lng"]
        else:
            x["formatted_address"] = None
            x["latitude"] = None
            x["longitude"] = None
        return x

    if not df.empty:
        df = df.apply(apply_geoinfo, axis=1)

    return df


def update_inactive_elements():
    q_read = """
        SELECT hc.id
        FROM houses_clean hc
        INNER JOIN houses_scrapper hs
        ON hc.id = hs.id
        WHERE NOT hs.active AND hc.active
    """
    df = execute_query(q_read)
    if not df.empty:
        logging.info(f"Updating {df.shape[0]} inactive elements")
        ids = df["id"].values.tolist()
        placeholders = ",".join(["%s"] * len(ids))
        q_update = f"UPDATE houses_clean SET active = false WHERE id IN ({placeholders});"
        execute_update_query(q_update, params=tuple(ids))


def pipeline():
    df = extract_scrapper_data()
    df = filter_df(df)
    df = add_geo_info(df)
    upload_to_table(df, "houses_clean")
    update_inactive_elements()


if __name__ == "__main__":
    start_t = datetime.datetime.now()
    pipeline()
    end_t = datetime.datetime.now()

    logging.info(f"Execution name: {end_t - start_t}")
