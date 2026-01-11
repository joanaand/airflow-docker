#Fetch historical snow and temperature data using Meteostat

import pandas as pd
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from meteostat import Point, Daily


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "ski_resorts_geocoded.csv"
OUTPUT_FILE = DATA_DIR / "ski_resorts_weather.csv"
FAILED_FILE = DATA_DIR / "weather_failures.csv"

REQUEST_DELAY = 1


def get_weather(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return None

    location = Point(lat, lon)

    end = datetime.now()
    start = end - timedelta(days=30)

    try:
        data = Daily(location, start, end)
        df = data.fetch()

        if df.empty:
            return None

        return {
            "temp_avg": df["tavg"].mean(),
            "snowfall_mm": df["snow"].sum(),
            "snow_depth_mm": df["tsnow"].mean()
        }

    except Exception as e:
        print(f"Weather fetch failed for ({lat}, {lon}): {e}")
        return None


def main():
    df = pd.read_csv(INPUT_FILE)

    temps = []
    snowfall = []
    snow_depth = []
    failed = []

    for i, row in df.iterrows():
        print(f"Weather {i + 1}/{len(df)}: {row['resort_name']}")

        weather = get_weather(row["latitude"], row["longitude"])

        if not weather:
            failed.append({
                "resort_name": row["resort_name"],
                "latitude": row["latitude"],
                "longitude": row["longitude"]
            })

            temps.append(None)
            snowfall.append(None)
            snow_depth.append(None)

        else:
            temps.append(weather["temp_avg"])
            snowfall.append(weather["snowfall_mm"])
            snow_depth.append(weather["snow_depth_mm"])

        time.sleep(REQUEST_DELAY)

    df["temp_avg"] = temps
    df["snowfall_30d_mm"] = snowfall
    df["snow_depth_avg_mm"] = snow_depth

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    if failed:
        pd.DataFrame(failed).to_csv(FAILED_FILE, index=False)
        print(f"Some weather data could not be fetched. See {FAILED_FILE}")

    print(f"Saved weather data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
