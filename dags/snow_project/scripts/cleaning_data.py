
import pandas as pd

file_path = "data/ski_resorts_weather.csv"
output_path = "data/ski_resorts_weather_clean.csv"

#read file
df = pd.read_csv(file_path)

#fix resort names and create sub_resorts column
cleaned_name = (
    df["resort_name"]
    .str.replace(r"^\d+\.\s*", "", regex=True)
    .str.replace("\u200b", "", regex=False)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

df[["resort_name", "sub_resorts"]] = (cleaned_name.str.split("–", n=1, expand=True))

#handle some missing data
df["sub_resorts"] = df["sub_resorts"].fillna("None")
df["weather_desc"] = df["weather_desc"].fillna("Unknown")
df["snow_mm"] = df["snow_mm"].fillna(0)

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
df.to_csv(output_path, index=False)


