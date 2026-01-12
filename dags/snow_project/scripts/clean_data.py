import pandas as pd
import pycountry
from pathlib import Path
import os
import re

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "ski_resorts_list.csv"
OUTPUT_FILE = DATA_DIR / "ski_resorts_clean.csv"


def get_country_code(country_name):
    if not isinstance(country_name, str):
        return None
    try:
        country = pycountry.countries.lookup(country_name)
        return country.alpha_2
    except LookupError:
        return None


def clean_name(text):
    if not isinstance(text, str):
        return None
    text = re.sub(r"\(.*?\)", "", text)      # remove brackets
    text = text.replace(")/", "/")
    text = re.sub(r"/.*", "", text)          # keep first part only
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_eur_price(price):
    if not isinstance(price, str):
        return None
    price = price.replace(",", ".")
    eur_match = re.search(r"€\s*(\d+(\.\d+)?)", price)
    return float(eur_match.group(1)) if eur_match else None


def main():
    df = pd.read_csv(INPUT_FILE)

    # --- Extract local names in brackets (without overwriting main name)
    extracted = df["resort_name"].str.extract(r"^(.*?)\s*\((.*?)\)$")
    df["local_name"] = extracted[1]

    # --- Clean resort names
    df["resort_name"] = df["resort_name"].apply(clean_name)

    # --- Split sub-resorts safely
    split_names = df["resort_name"].str.split("–", n=1, expand=True)
    df["resort_name"] = split_names[0].str.strip()
    df["sub_resorts"] = split_names[1].str.strip()

    # --- Clean sub-resorts & local names
    df["sub_resorts"] = df["sub_resorts"].apply(clean_name)
    df["local_name"] = df["local_name"].apply(clean_name)

    # --- Clean town names
    df["town_clean"] = (
        df["town"]
        .str.replace(r"\s+in\s+.*", "", regex=True)
        .str.replace(r"\s+am\s+.*", "", regex=True)
        .str.strip()
    )

    # --- Rebuild missing resort names from town
    df["resort_name"] = df["resort_name"].fillna(df["town_clean"])

    # --- Country codes
    df["country_code"] = df["country"].apply(get_country_code)

    # --- Prices
    df["lift_price_eur"] = df["lift_price"].apply(extract_eur_price)

    # --- Price per km
    df["price_per_km_eur"] = (df["lift_price_eur"] / df["total_km"]).round(2)

    # --- Difficulty score
    df["difficulty_score"] = (
        (df["blue_km"] * 1 + df["red_km"] * 2 + df["black_km"] * 3) / df["total_km"]
    ).round(3)

    # --- Elevation bands
    df["elevation_band"] = pd.cut(
        df["elevation_m"],
        bins=[0, 800, 1200, 1600, 3000],
        labels=["Low", "Medium", "High", "Very High"]
    )

    # --- Save
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Cleaned data saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
