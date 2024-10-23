# from geopy.geocoders import Nominatim

# geolocator = Nominatim(timeout=3, user_agent="test_geo")

# location = [synthetic_latitudes[0], synthetic_longitudes[0]]
# geoloc = geolocator.reverse(location, language='es')

# geoloc

# from geopy.geocoders import Nominatim
# geolocator = Nominatim(user_agent="geolocation_test", timeout=10)

# def city_state_country(row):
#     coord = f"{row['latitude']}, {row['longitude']}"
#     location = geolocator.reverse(coord, exactly_one=True)
#     address = location.raw['address']
#     city = address.get('city', '')
#     state = address.get('state', '')
#     country = address.get('country', '')
#     row['city'] = city
#     row['state'] = state
#     row['country'] = country
#     return row

# df_test_geo = pd.DataFrame({"latitude": lat_genhyperbolic_data, "longitude": lon_genhyperbolic_data})
# df_test_geo = df_test_geo.sample(25).apply(city_state_country, axis=1)
# print(df_test_geo)

# address = geoloc.raw['address']
# city = address.get('city', '')

# address