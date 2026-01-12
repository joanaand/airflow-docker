import requests
import pandas as pd
import time
import re
import os
from pathlib import Path
#from airflow.models import Variable

try:
    from airflow.models import Variable
    API_KEY = Variable.get("OPENWEATHER_API_KEY")
except:
    import os
    API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not found in Airflow Variables or environment variables")


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "ski_resorts_clean.csv"
OUTPUT_FILE = DATA_DIR / "ski_resorts_geocoded.csv"
FAILED_FILE = DATA_DIR / "geocode_failures.csv"

GEOCODE_URL = "http://api.openweathermap.org/geo/1.0/direct"
REQUEST_DELAY = 1


def normalize_place(text):
    if not text:
        return None
    text = re.sub(r"\(.*?\)", "", text)       #remove brackets
    text = re.sub(r"-.*", "", text)           #remove suffixes
    text = re.sub(r"\d{4}", "", text)         #remove postal codes
    text = text.replace("/", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

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
    if abs(lat) > 70:
        return False
    return True

def build_candidates(row):
    return [
        normalize_place(row.get("town_clean")) if isinstance(row.get("town_clean"), str) else None,
        normalize_place(row.get("town")) if isinstance(row.get("town"), str) else None,
        normalize_place(row.get("resort_name")) if isinstance(row.get("resort_name"), str) else None,
        normalize_place(row.get("sub_resorts")) if isinstance(row.get("sub_resorts"), str) else None,
        normalize_place(row.get("local_name")) if isinstance(row.get("local_name"), str) else None,
        f"{normalize_place(row.get('resort_name'))} ski resort"
        if isinstance(row.get("resort_name"), str) else None
    ]


def geocode_location(row):
    country_code = row.get("country_code")

    if not isinstance(country_code, str) or not country_code.strip():
        return None, None, "missing_country"

    candidates = build_candidates(row)

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
        name = row["resort_name"] if isinstance(row["resort_name"], str) else row["town"]
        print(f"Geocoding {i + 1}/{len(df)}: {name}")

        lat, lon, source = geocode_location(row)

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
