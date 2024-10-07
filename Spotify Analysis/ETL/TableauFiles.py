import os
import platform

import pandas as pd

from common import read_full_dynamo_table

top_artists = read_full_dynamo_table("top_artists")
recently_played = read_full_dynamo_table("recently_played")

top_artists = pd.DataFrame(top_artists)
recently_played = pd.DataFrame(recently_played)

if platform.system() == "Windows":
    os.chdir("..")
    top_artists.to_csv("Analysis\\top_artists_last.csv")
    recently_played.to_csv("Analysis\\recently_played_last.csv")
else:
    pass
    # if executed in a cloud service, file management should be included here.
    # Files could be uploaded to s3 or another service to later be consumed by Tableau.
