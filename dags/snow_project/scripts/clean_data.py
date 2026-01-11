
import pandas as pd
import pycountry
from pathlib import Path
import os

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
    
def main():
    #read file
    df = pd.read_csv(INPUT_FILE)

    #fix resort names and create sub_resorts column
    cleaned_name = (
        df["resort_name"]
        .str.replace(r"^\d+\.\s*", "", regex=True)
        .str.replace("\u200b", "", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    df[["resort_name", "sub_resorts"]] = (cleaned_name.str.split("–", n=1, expand=True))
    df["resort_name"] = df["resort_name"].str.strip()
    df["sub_resorts"] = df["sub_resorts"].str.strip()

    #handle some missing data
    df["sub_resorts"] = df["sub_resorts"].fillna("None")


    #add country codes
    df["country_code"] = df["country"].apply(get_country_code)

    #extract lift price in euros as float
    df["lift_price_eur"] = (
        df["lift_price"]
        .str.replace(",", ".")
        .str.extract(r"€\s*(\d+(\.\d+)?)")[0]
        .astype(float)
    )

    #calculate price per km
    df["price_per_km_eur"] = (df["lift_price_eur"] / df["total_km"]).round(2)


    #calculate difficulty score
    df["difficulty_score"] = ((df["blue_km"] * 1 + df["red_km"] * 2 + df["black_km"] * 3) / df["total_km"]).round(3)

    #save cleaned data
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
