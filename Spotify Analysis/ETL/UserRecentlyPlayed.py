from common import get_spotipy_client, load_to_dynamo, user_recently_played_scope


def get_recently_played(limit=25):
    sp = get_spotipy_client(user_recently_played_scope)
    response = sp.current_user_recently_played(limit=limit)

    results = []
    for track in response["items"]:
        track_info = {
            "track_name": track["track"]["name"],
            "album": track["track"]["album"]["name"],
            "artist": track["track"]["artists"][0]["name"],
            "url": track["track"]["external_urls"]["spotify"],
        }
        results.append(track_info)

    results = [dict(t) for t in {tuple(d.items()) for d in results}]
    return results


results = get_recently_played()
load_to_dynamo(results, "recently_played", empty_table=True)
