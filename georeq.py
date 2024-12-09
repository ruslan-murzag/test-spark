import requests

API_KEY = "0f1c9eb4a9054e50b691f4a55391ac68"


def get_coordinates(city, country):
    url = f"https://api.opencagedata.com/geocode/v1/json?q={city},{country}&key={API_KEY}"
    response = requests.get(url).json()
    if response.get("results"):
        coords = response["results"][0]["geometry"]
        return coords["lat"], coords["lng"]
    return None, None

# city = "Dillon"
# country = "US"
# latitude, longitude = get_coordinates(city, country)
#
# print(f"Coordinates for {city}, {country}: Latitude = {latitude}, Longitude = {longitude}")

