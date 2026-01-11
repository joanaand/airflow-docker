import requests
import pandas as pd
import time
import os
from pathlib import Path
from airflow.models import Variable

#API_KEY = os.getenv("OPENWEATHER_API_KEY") - for testing locally
API_KEY = Variable.get("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not found in environment variables")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "ski_resorts_clean.csv"
OUTPUT_FILE = DATA_DIR / "ski_resorts_geocoded.csv"
FAILED_FILE = DATA_DIR / "geocode_failures.csv"

GEOCODE_URL = "http://api.openweathermap.org/geo/1.0/direct"
REQUEST_DELAY = 1


def geocode_location(town, country_code):
    """
    Geocode using clean town + ISO country code.
    Example: Morillon,FR
    """

    if not town or not country_code:
        return None, None

    query = f"{town},{country_code}"

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
        print(f"Geocoding failed for '{query}': {e}")

    return None, None


def main():
    df = pd.read_csv(INPUT_FILE)

    latitudes = []
    longitudes = []
    failed = []

    for i, row in df.iterrows():
        print(f"Geocoding {i + 1}/{len(df)}: {row['resort_name']}")

        lat, lon = geocode_location(
            town=row["town"],
            country_code=row["country_code"]
        )

        if lat is None:
            failed.append({
                "resort_name": row["resort_name"],
                "town": row["town"],
                "country_code": row["country_code"]
            })

        latitudes.append(lat)
        longitudes.append(lon)

        time.sleep(REQUEST_DELAY)

    df["latitude"] = latitudes
    df["longitude"] = longitudes
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    if failed:
        pd.DataFrame(failed).to_csv(FAILED_FILE, index=False)
        print(f"Some resorts could not be geocoded. See {FAILED_FILE}")

    print(f"Saved geocoded data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
