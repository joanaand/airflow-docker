import requests
import pandas as pd
import time
import os

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not found in environment variables")

INPUT_FILE = "data/ski_resorts_list.csv"
OUTPUT_FILE = "data/ski_resorts_geocoded.csv"

GEOCODE_URL = "http://api.openweathermap.org/geo/1.0/direct"
REQUEST_DELAY = 1  # seconds


def geocode_location(resort_name, town, country):
    """
    Attempts to geocode a ski resort using multiple query strategies.
    Returns (latitude, longitude) or (None, None) if not found.
    """

    # Require at least some meaningful location information
    if not (resort_name or town):
        return None, None

    queries = [
        f"{town}, {country}" if town and country else None,
        f"{resort_name}, {country}" if resort_name and country else None,
        town,
        resort_name
    ]

    # Remove empty queries
    queries = [q for q in queries if q]

    for query in queries:
        params = {
            "q": query,
            "limit": 5,
            "appid": API_KEY
        }

        try:
            response = requests.get(GEOCODE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data:
                return data[0]["lat"], data[0]["lon"]

        except requests.RequestException as e:
            print(f"Geocoding request failed for '{query}': {e}")

    print(f"Could not geocode: {resort_name} ({town}, {country})")
    return None, None


def main():
    df = pd.read_csv(INPUT_FILE)

    latitudes = []
    longitudes = []

    for i, row in df.iterrows():
        print(f"Geocoding {i + 1}/{len(df)}: {row['resort_name']}")

        lat, lon = geocode_location(
            resort_name=row["resort_name"],
            town=row["town"],
            country=row["country"]
        )

        latitudes.append(lat)
        longitudes.append(lon)

        time.sleep(REQUEST_DELAY)

    df["latitude"] = latitudes
    df["longitude"] = longitudes

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved geocoded data to {OUTPUT_FILE}")
    print(df.head())


if __name__ == "__main__":
    main()
