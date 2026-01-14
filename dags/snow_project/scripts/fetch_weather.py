import requests
import pandas as pd
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "ski_resorts_geocoded.csv"
OUTPUT_FILE = DATA_DIR / "ski_resorts_weather.csv"
FAILED_FILE = DATA_DIR / "weather_failures.csv"

REQUEST_DELAY = 1


def fetch_open_meteo(lat, lon):
    end_date = end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=30)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": (
            "snowfall_sum,"
            "snow_depth_mean,"
            "temperature_2m_mean,"
            "sunshine_duration,"
            "rain_sum"
        ),
        "timezone": "UTC"
    }

    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        data = res.json().get("daily", {})

        if not data:
            return None

        snowfall = data.get("snowfall_sum", [])
        snow_depth = data.get("snow_depth_mean", [])
        temps = data.get("temperature_2m_mean", [])
        sunshine = data.get("sunshine_duration", [])
        rain = data.get("rain_sum", [])

        return {
            "total_snowfall_mm": sum(snowfall) if snowfall else None,
            "avg_snow_depth_mm": sum(snow_depth) / len(snow_depth) if snow_depth else None,
            "avg_temp_30d_c": sum(temps) / len(temps) if temps else None,
            "total_rain_30d_mm": sum(rain) if rain else None,
            "avg_sunshine_hours_30d": (
                sum(sunshine) / len(sunshine) / 3600 if sunshine else None
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
    sunshine = []
    rain = []
    failed = []

    for i, row in df.iterrows():
        print(f"Fetching weather {i + 1}/{len(df)} for {row['resort_name']}")

        # Skip resorts with missing coordinates
        if pd.isna(row["latitude"]) or pd.isna(row["longitude"]):
            print("Skipping resort with missing coordinates")

            snowfall.append(None)
            snow_depth.append(None)
            avg_temp.append(None)
            sunshine.append(None)
            rain.append(None)

            failed.append({
                "resort_name": row["resort_name"],
                "latitude": row["latitude"],
                "longitude": row["longitude"]
            })
            continue

        result = fetch_open_meteo(row["latitude"], row["longitude"])

        if not result:

            snowfall.append(None)
            snow_depth.append(None)
            avg_temp.append(None)
            sunshine.append(None)
            rain.append(None)

            failed.append({
                "resort_name": row["resort_name"],
                "latitude": row["latitude"],
                "longitude": row["longitude"]
            })

        else:
            snowfall.append(result["total_snowfall_mm"])
            snow_depth.append(result["avg_snow_depth_mm"])
            avg_temp.append(result["avg_temp_30d_c"])
            sunshine.append(result["avg_sunshine_hours_30d"])
            rain.append(result["total_rain_30d_mm"])

        time.sleep(REQUEST_DELAY)

    df["total_snowfall_30d_mm"] = snowfall
    df["avg_snow_depth_30d_mm"] = snow_depth
    df["avg_temp_30d_c"] = avg_temp
    df["avg_sunshine_hours_30d"] = sunshine
    df["total_rain_30d_mm"] = rain

    df.to_csv(OUTPUT_FILE, index=False)

    if failed:
        pd.DataFrame(failed).to_csv(FAILED_FILE, index=False)
        print(f"Some weather fetches failed. See {FAILED_FILE}")

    print("Weather fetch complete. Saved to", OUTPUT_FILE)


if __name__ == "__main__":
    main()
