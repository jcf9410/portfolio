from statistics import mean

from common import get_spotipy_client, load_to_dynamo, parse_numeric_data, user_lib_scope

valid_audio_features = ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness",
                        "instrumentalness", "liveness", "valence", "tempo"]

_limit = 50


def get_saved_tracks():
    sp = get_spotipy_client(user_lib_scope)

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
        try:
            if abs(past_element[feature] - element[feature]) / element[feature] > 0.05:  # 5% change
                return True
        except ZeroDivisionError:
            return False
    return False


def get_audio_features(tracks):
    sp = get_spotipy_client(user_lib_scope)
    chunks = [tracks[i:i + 100] for i in range(0, len(tracks), 100)]
    features_results = []

    for chunk in chunks:
        ids = [e["track_id"] for e in chunk]
        features = sp.audio_features(ids)
        features_results.extend(features)

    results = []
    for track, feature in zip(tracks, features_results):
        new_feature = {k: v for k, v in feature.items() if k in valid_audio_features}
        new_feature = {**track, **new_feature}
        results.append(new_feature)

    return results


def get_advanced_audio_features(track):
    sp = get_spotipy_client(user_lib_scope)
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
        if abs(sections[i - 1]["loudness"] - sections[i]["loudness"]) / sections[i][
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


all_tracks = get_saved_tracks()

results = []

for track in all_tracks:
    clean_track = clean_data(track)
    clean_track = get_advanced_audio_features(clean_track)

    results.append(clean_track)

results = get_audio_features(results)

parse_numeric_data(results)

load_to_dynamo(results, "track_info")
