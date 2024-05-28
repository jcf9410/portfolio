from decimal import Context

import boto3
from statistics import mean
import spotipy
from spotipy.oauth2 import SpotifyOAuth

import time

start_time = time.time()


valid_audio_features = ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness",
                        "instrumentalness", "liveness", "valence", "tempo"]
user_lib_scope = "user-library-read"
_redirect_uri = "http://localhost:8889/callback"

_limit = 30

def get_saved_tracks():
    ssm = boto3.client("ssm", region_name="eu-west-1")
    client_secret = ssm.get_parameter(Name="SPOTIFY_CLIENT_SECRET", WithDecryption=True)
    client_secret = client_secret["Parameter"]["Value"]
    client_id = ssm.get_parameter(Name="SPOTIFY_CLIENT_ID", WithDecryption=True)
    client_id = client_id["Parameter"]["Value"]

    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(scope=user_lib_scope, client_id=client_id, client_secret=client_secret,
                                  redirect_uri=_redirect_uri))

    all_results = []

    offset = 0
    results = sp.current_user_saved_tracks(limit=_limit)
    while results["next"] is not None:
        if offset > 0:
            results = sp.current_user_saved_tracks(limit=_limit, offset=offset)
        for idx, item in enumerate(results['items']):
            track = item['track']
            all_results.append(track)
        offset += _limit

    return all_results


def clean_data(track):
    track_id = track["id"]
    album = track["album"]["name"]
    artist = track["artists"][0]["name"]
    duration = track["duration_ms"] / 1000
    explicit = track["explicit"]
    name = track["name"]
    popularity = track["popularity"]

    track_info = {
        "track_id": track_id,
        "album": album,
        "artist": artist,
        "duration": duration,
        "explicit": explicit,
        "track_name": name,
        "popularity": popularity
    }

    return track_info


def get_change_in_feature(element, past_element, feature):
    if past_element[f"{feature}_confidence"] > 0.5 and element[f"{feature}_confidence"] > 0.5:  # enough confidence
        if abs(past_element[feature] - element[feature]) / element[feature] > 0.05:  # 5% change
            return True
    return False


def get_audio_features(tracks):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    client_secret = ssm.get_parameter(Name="SPOTIFY_CLIENT_SECRET", WithDecryption=True)
    client_secret = client_secret["Parameter"]["Value"]
    client_id = ssm.get_parameter(Name="SPOTIFY_CLIENT_ID", WithDecryption=True)
    client_id = client_id["Parameter"]["Value"]

    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(scope=user_lib_scope, client_id=client_id, client_secret=client_secret,
                                  redirect_uri=_redirect_uri))

    # track_id = track["track_id"]
    chunks = [tracks[i:i + 100] for i in range(0, len(tracks), 100)]
    for chunk in chunks:
        ids = [e["track_id"] for e in chunk]
        features = sp.audio_features(ids)

    return
    # while True:
    # try:
    #     features = sp.audio_features(track_id)
    # except spotipy.client.SpotifyException as e:
    #     print(repr(e))
    #     raise e
    # features = {k: v for k, v in features[0].items() if k in valid_audio_features}

    # track_info = {**track, **features}

    # return track_info


def get_advanced_audio_features(track):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    client_secret = ssm.get_parameter(Name="SPOTIFY_CLIENT_SECRET", WithDecryption=True)
    client_secret = client_secret["Parameter"]["Value"]
    client_id = ssm.get_parameter(Name="SPOTIFY_CLIENT_ID", WithDecryption=True)
    client_id = client_id["Parameter"]["Value"]

    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(scope=user_lib_scope, client_id=client_id, client_secret=client_secret,
                                  redirect_uri=_redirect_uri))

    track_id = track["track_id"]
    features = sp.audio_analysis(track_id)
    sections = features["sections"]

    track_info = {**track, "raw_sections": sections}

    num_sections = len(sections)
    sections_avg_duration = mean([e["duration"] for e in sections])

    tempo_changes = 0
    key_changes = 0
    mode_changes = 0
    time_signature_changes = 0
    dynamics_changes = 0

    for i in range(1, len(sections)):
        if get_change_in_feature(sections[i], sections[i - 1], "tempo"):
            tempo_changes += 1
        if get_change_in_feature(sections[i], sections[i - 1], "key"):
            key_changes += 1
        if get_change_in_feature(sections[i], sections[i - 1], "mode"):
            mode_changes += 1
        if get_change_in_feature(sections[i], sections[i - 1], "time_signature"):
            time_signature_changes += 1
        if np.abs(sections[i - 1]["loudness"] - sections[i]["loudness"]) / sections[i][
            "loudness"] > 0.1:  # no confidence, wider interval
            dynamics_changes += 1

    track_info["num_sections"] = num_sections
    track_info["sections_avg_duration"] = sections_avg_duration
    track_info["tempo_changes"] = tempo_changes
    track_info["key_changes"] = key_changes
    track_info["mode_changes"] = mode_changes
    track_info["time_signature_changes"] = time_signature_changes
    track_info["dynamics_changes"] = dynamics_changes

    return track_info


def parse_numeric_data(data):
    ctx = Context(prec=38)

    for entry in data:
        for k, v in entry.items():
            if type(v) in (float, int, np.number, np.float64, np.int64):
                entry[k] = ctx.create_decimal_from_float(v)
            elif isinstance(v, list):
                for e in v:
                    for k2, v2 in e.items():
                        e[k2] = ctx.create_decimal_from_float(v2)


def load_to_dynamo(data):
    session = boto3.Session(profile_name="default")

    dynamodb = session.resource("dynamodb", region_name="eu-west-1")
    table = dynamodb.Table("track_info")

    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(Item=item)


all_tracks = get_saved_tracks()

results = []

for track in all_tracks:
    clean_track = clean_data(track)
    # clean_track = get_advanced_audio_features(clean_track)

    results.append(clean_track)

results = get_audio_features(results)

parse_numeric_data(results)

# load_to_dynamo(results)

end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time} seconds ({elapsed_time / 60} min)")