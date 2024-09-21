from common import get_spotipy_client, load_to_dynamo, parse_numeric_data, user_top_scope
import uuid

def get_top_artists(limit=20):
    sp = get_spotipy_client(user_top_scope)
    results = []

    for time_range in ("short_term", "medium_term", "long_term"):
        response = sp.current_user_top_artists(time_range=time_range, limit=limit)

        for artist in response["items"]:
            artist_info = {
                "id": str(uuid.uuid4()),
                "artist_name": artist["name"],
                "genres": artist["genres"],
                "followers": artist["followers"]["total"],
                "time_range": time_range,
                "img": artist["images"][0]["url"],
                "url": artist["external_urls"]["spotify"]
            }
            results.append(artist_info)

    return results


results = get_top_artists()
parse_numeric_data(results)
load_to_dynamo(results, "top_artists", empty_table=True)
