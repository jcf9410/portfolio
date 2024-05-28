from decimal import Context
import platform

import boto3
import spotipy
from spotipy.oauth2 import SpotifyOAuth

user_lib_scope = "user-library-read"
user_top_scope = "user-top-read"
user_recently_played_scope = "user-read-recently-played"

scope_mapping = {
    user_lib_scope: "SPOTIFY_REFRESH_TOKEN_LIBRARY_READ",
    user_top_scope: "SPOTIFY_REFRESH_TOKEN_TOP_READ",
    user_recently_played_scope: "SPOTIFY_REFRESH_TOKEN_RECENTLE_PLAYED"
}
redirect_uri = "http://localhost:8889/callback"

if platform.system() == "Windows":
    cache_path = None
    profile = "default"
else:
    cache_path = "/tmp/.cache"
    profile = None

def parse_numeric_data(data):
    ctx = Context(prec=38)

    for entry in data:
        for k, v in entry.items():
            if type(v) in (float, int):
                entry[k] = ctx.create_decimal_from_float(v)
            elif isinstance(v, list):
                for e in v:
                    if isinstance(e, dict):
                        for k2, v2 in e.items():
                            e[k2] = ctx.create_decimal_from_float(v2)


def get_spotipy_client(scope):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    client_secret = ssm.get_parameter(Name="SPOTIFY_CLIENT_SECRET", WithDecryption=True)
    client_secret = client_secret["Parameter"]["Value"]
    client_id = ssm.get_parameter(Name="SPOTIFY_CLIENT_ID", WithDecryption=True)
    client_id = client_id["Parameter"]["Value"]
    refresh_token = ssm.get_parameter(Name=scope_mapping[scope], WithDecryption=True)
    refresh_token = refresh_token["Parameter"]["Value"]

    sp_oauth = SpotifyOAuth(client_id, client_secret, redirect_uri, scope=scope, cache_path=cache_path)
    token_info = sp_oauth.refresh_access_token(refresh_token)
    sp = spotipy.Spotify(auth=token_info["access_token"])

    return sp


def load_to_dynamo(data, table_name, empty_table=None):
    session = boto3.Session(profile_name=profile)

    dynamodb = session.resource("dynamodb", region_name="eu-west-1")
    table = dynamodb.Table(table_name)

    if empty_table:
        empty_dynamo_table(table)

    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(Item=item)


def empty_dynamo_table(table, key=None):
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan["Items"]:
            batch.delete_item(
                Key={
                    "artist_name": each["artist_name"]  # Replace "PrimaryKey" with your actual primary key name
                }
            )
        while "LastEvaluatedKey" in scan:
            scan = table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
            for each in scan["Items"]:
                batch.delete_item(
                    Key={
                        "artist_name": each["artist_name"]  # Replace "PrimaryKey" with your actual primary key name
                    }
                )
