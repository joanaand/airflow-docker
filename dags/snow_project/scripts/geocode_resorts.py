import requests
import pandas as pd
import time
import os

GEOCODE_URL = "http://api.openweathermap.org/geo/1.0/direct"
API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not found in environment variables")

INPUT_FILE = "ski_resorts_list.csv"
OUTPUT_FILE = "ski_resorts_geocoded.csv"

REQUEST_DELAY = 1  # seconds

def geocode_location(town, country):
    if not town or not country:
        return None, None
    
    query = f"{town},{country}"

    params = {
        "q": query,
        "limit": 1,
        "appid": API_KEY
    }

    try:
        response = requests.get(GEOCODE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data:
            return data[0]["lat"], data[0]["lon"]
    
    except requests.RequestException as e:
        print(f"Geocoding request failed for {query}: {e}")
    
    return None, None


    def main():
        df = pd.read_csv(INPUT_FILE)

        latitudes = []
        longitudes = []

        for i, row in df.iterrows():
            print(f"Geocoding {i+1}/{len(df)}: {row['town']}, {row['country']}")

            lat, lon = geocode_location(row["town"], row["country"])
            latitudes.append(lat)
            longitudes.append(lon)

            time.sleep(REQUEST_DELAY)

        df["latitude"] = latitudes
        df["longitude"] = longitudes
            
        df.to_csv(OUTPUT_FILE, index=False)

        print(f"\nSaved geocoded data to {OUTPUT_FILE}")
        print(df.head())

if __name__ == "__main__":
    main()