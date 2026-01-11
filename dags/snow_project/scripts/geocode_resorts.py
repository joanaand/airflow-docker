import requests
import pandas as pd
import time
import re
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


def normalize_town(town):
    if not town:
        return None
    town = re.sub(r"\d{4}", "", town)
    town = re.sub(r"The Village at ", "", town, flags=re.I)
    town = re.sub(r"-.*", "", town)
    town = re.sub(r"\(.*?\)", "", town)
    town = town.strip()
    return town


def extract_primary_town(resort_name):
    if not resort_name:
        return None
    name = re.sub(r"[–-].*", "", resort_name)
    name = re.sub(r"\(.*?\)", "", name)
    name = name.split("/")[0]
    return name.strip()


def geocode_query(query, country_code):
    params = {
        "q": f"{query},{country_code}",
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


def is_valid_coordinates(lat, lon):
    if lat is None or lon is None:
        return False
    if abs(lat) > 65:
        return False
    return True


def geocode_location(town, resort_name, country_code):
    if not country_code:
        return None, None, "missing_country"

    normalized_town = normalize_town(town)
    primary_town = extract_primary_town(resort_name)

    candidates = [
        normalized_town,
        town,
        primary_town,
        f"{primary_town} ski resort" if primary_town else None
    ]

    for query in candidates:
        if not query:
            continue
        lat, lon = geocode_query(query, country_code)
        if is_valid_coordinates(lat, lon):
            return lat, lon, query

    return None, None, "not_found"


def main():
    df = pd.read_csv(INPUT_FILE)

    latitudes = []
    longitudes = []
    sources = []
    failed = []

    for i, row in df.iterrows():
        print(f"Geocoding {i + 1}/{len(df)}: {row['resort_name']}")

        lat, lon, source = geocode_location(
            town=row["town"],
            resort_name=row["resort_name"],
            country_code=row["country_code"]
        )

        if lat is None:
            failed.append({
                "resort_name": row["resort_name"],
                "town": row["town"],
                "country_code": row["country_code"],
                "reason": source
            })

        latitudes.append(lat)
        longitudes.append(lon)
        sources.append(source)

        time.sleep(REQUEST_DELAY)

    df["latitude"] = latitudes
    df["longitude"] = longitudes
    df["geocode_source"] = sources

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    if failed:
        pd.DataFrame(failed).to_csv(FAILED_FILE, index=False)
        print(f"Some resorts could not be geocoded. See {FAILED_FILE}")

    print(f"Saved geocoded data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
