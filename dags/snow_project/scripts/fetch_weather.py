import requests
import pandas as pd
import time
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "ski_resorts_geocoded.csv"
OUTPUT_FILE = DATA_DIR / "ski_resorts_weather.csv"
FAILED_FILE = DATA_DIR / "weather_failures.csv"

REQUEST_DELAY = 1

def fetch_open_meteo(lat, lon):
    """
    Fetch last 30 days of snow and temperature from Open-Meteo.
    """
    # Define date range
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=30)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "snowfall_sum,snow_depth_mean,temperature_2m_max",
        "timezone": "UTC"
    }

    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        data = res.json().get("daily", {})

        # If no usable data, return None
        if not data:
            return None

        # Build results
        return {
            "total_snowfall_mm": sum(data.get("snowfall_sum", [])),
            "avg_snow_depth_mm": (
                sum(data.get("snow_depth_mean", [])) / len(data.get("snow_depth_mean", []))
                if data.get("snow_depth_mean")
                else None
            ),
            "avg_max_temp_30d_c": (
                sum(data.get("temperature_2m_max", [])) / len(data.get("temperature_2m_max", []))
                if data.get("temperature_2m_max")
                else None
            )
        }
    except Exception as e:
        print(f"Open-Meteo fetch failed for {lat},{lon}: {e}")
        return None


def main():
    df = pd.read_csv(INPUT_FILE)

    snowfall = []
    snow_depth = []
    avg_temp = []
    failed = []

    for i, row in df.iterrows():
        print(f"Fetching weather {i + 1}/{len(df)} for {row['resort_name']}")

        result = fetch_open_meteo(row["latitude"], row["longitude"])

        if not result:
            failed.append({
                "resort_name": row["resort_name"],
                "latitude": row["latitude"],
                "longitude": row["longitude"]
            })
            snowfall.append(None)
            snow_depth.append(None)
            avg_temp.append(None)
        else:
            snowfall.append(result["total_snowfall_mm"])
            snow_depth.append(result["avg_snow_depth_mm"])
            avg_temp.append(result["avg_max_temp_30d_c"])

        time.sleep(REQUEST_DELAY)

    df["total_snowfall_30d_mm"] = snowfall
    df["avg_snow_depth_30d_mm"] = snow_depth
    df["avg_max_temp_30d_c"] = avg_temp

    df.to_csv(OUTPUT_FILE, index=False)

    if failed:
        pd.DataFrame(failed).to_csv(FAILED_FILE, index=False)
        print(f"Some weather fetches failed. See {FAILED_FILE}")

    print("Weather fetch complete. Saved to", OUTPUT_FILE)


if __name__ == "__main__":
    main()
