import requests
import pandas as pd
import time
import os

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not found in environment variables")

INPUT_FILE = "data/ski_resorts_geocoded.csv"
OUTPUT_FILE = "data/ski_resorts_weather.csv"

WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
REQUEST_DELAY = 1  # seconds

def get_weather(lat, lon):
    params = {
        "lat": lat,
        "lon": lon,
        "exclude": "minutely,hourly,alerts",
        "units": "metric",
        "appid": API_KEY
    }

    try:
        r = requests.get(WEATHER_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        return {
            "temp_day": data["main"]["temp"],
            "snow": data.get("snow", {}).get("1h", 0),
            "weather": data["weather"][0]["description"]
}

    except requests.RequestException:
        return {"temp_day": None, "snow": None, "weather": None}


def main():
    df = pd.read_csv(INPUT_FILE)

    temps, snows, conditions = [], [], []

    for i, row in df.iterrows():
        print(f"Weather {i+1}/{len(df)}: {row['resort_name']}")

        weather = get_weather(row["latitude"], row["longitude"])

        temps.append(weather["temp_day"])
        snows.append(weather["snow"])
        conditions.append(weather["weather"])

        time.sleep(REQUEST_DELAY)

    df["temp_day"] = temps
    df["snow_mm"] = snows
    df["weather_desc"] = conditions

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved weather data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()