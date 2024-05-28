from common import get_spotipy_client, load_to_dynamo, user_recently_played_scope


def get_recently_played(limit=20):
    sp = get_spotipy_client(user_recently_played_scope)
    response = sp.current_user_recently_played(limit=limit)

    results = []
    for track in response["items"]:
        track_info = {
            "track_name": track["name"],
            "album": track["album"],
            "artist": track["artists"][0]["name"]
        }
        results.append(track_info)

    return results


results = get_recently_played()
load_to_dynamo(results, "recently_played", empty_table=True)
