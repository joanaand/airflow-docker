import requests
import pandas as pd
import time
import os
from pathlib import Path

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not found in environment variables")

BASE_DIR = Path(__file__).resolve().parents[1]  #dags/snow_project
DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "ski_resorts_geocoded.csv"
OUTPUT_FILE = DATA_DIR / "ski_resorts_weather.csv"
FAILED_FILE = DATA_DIR / "weather_failures.csv"

WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
REQUEST_DELAY = 1  #seconds


def get_weather(lat, lon):
    """Fetch current weather data for given coordinates."""

    if pd.isna(lat) or pd.isna(lon):
        return {"temp_day": None, "snow": None, "weather": None}

    params = {
        "lat": lat,
        "lon": lon,
        "units": "metric",
        "appid": API_KEY
    }

    try:
        response = requests.get(WEATHER_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        return {
            "temp_day": data["main"]["temp"],
            "snow": data.get("snow", {}).get("1h", 0),
            "weather": data["weather"][0]["description"]
        }

    except requests.RequestException as e:
        print(f"Weather request failed for ({lat}, {lon}): {e}")
        return {"temp_day": None, "snow": None, "weather": None}


def main():
    df = pd.read_csv(INPUT_FILE)

    temps = []
    snows = []
    conditions = []
    failed = []

    for i, row in df.iterrows():
        print(f"Weather {i + 1}/{len(df)}: {row['resort_name']}")

        weather = get_weather(row["latitude"], row["longitude"])

        if weather["temp_day"] is None:
            failed.append({
                "resort_name": row["resort_name"],
                "latitude": row["latitude"],
                "longitude": row["longitude"]
            })

        temps.append(weather["temp_day"])
        snows.append(weather["snow"])
        conditions.append(weather["weather"])

        time.sleep(REQUEST_DELAY)

    df["temp_day"] = temps
    df["snow_mm"] = snows
    df["weather_desc"] = conditions

    df.to_csv(OUTPUT_FILE, index=False)

    if failed:
        pd.DataFrame(failed).to_csv(FAILED_FILE, index=False)
        print(f"Some weather data could not be fetched. See {FAILED_FILE}")

    print(f"Saved weather data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
