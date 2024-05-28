from common import get_spotipy_client, load_to_dynamo, parse_numeric_data, user_top_scope


def get_top_artists(limit=20):
    sp = get_spotipy_client(user_top_scope)
    response = sp.current_user_top_artists(time_range="long_term", limit=limit)

    results = []
    for artist in response["items"]:
        artist_info = {
            "artist_name": artist["name"],
            "genres": artist["genres"],
            "followers": artist["followers"]["total"]
        }
        results.append(artist_info)

    return results


results = get_top_artists()
parse_numeric_data(results)
load_to_dynamo(results, "top_artists", empty_table=True)
