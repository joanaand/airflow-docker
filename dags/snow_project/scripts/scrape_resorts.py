#Scrape ski resort list from skiresort.info
#Extract: Name, Country, URL, Total km, Blue/Red/Black km, Lift price, Town, Elevation

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from pathlib import Path
import os


BASE_URL = "https://www.skiresort.info"
LIST_URL = f"{BASE_URL}/ski-resorts/sorted/slope-length/"
PAGE_URL = f"{BASE_URL}/ski-resorts/page/{{page}}/sorted/slope-length/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataEngineeringBot/1.0)"
}

RESORT_COUNT = 100
REQUEST_TIMEOUT = 10
REQUEST_DELAY = 2  #seconds

#Path handling
BASE_DIR = Path(__file__).resolve().parents[1]  #dags/snow_project
DATA_DIR = BASE_DIR / "data"
OUTPUT_FILE = DATA_DIR / "ski_resorts_list.csv"


def clean_text(text):
    return re.sub(r"\s+", " ", text).strip() if text else None


def extract_number(text):
    if not text:
        return None
    match = re.search(r"([\d,.]+)", text.replace(",", ""))
    return float(match.group(1)) if match else None


def extract_main_town(resort_url):
    try:
        response = requests.get(resort_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException:
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    town_link = soup.select_one("ul.detail-overview-citylist li a")
    if town_link:
        town_text = town_link.get_text(strip=True)
        return town_text.split("(")[0].strip()

    return None


def scrape_resort_list():
    resorts = []
    page = 1

    while len(resorts) < RESORT_COUNT:
        url = LIST_URL if page == 1 else PAGE_URL.format(page=page)
        print(f"Scraping list page: {url}")

        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        resort_cards = soup.select("div.resort-list-item")

        for card in resort_cards:
            if len(resorts) >= RESORT_COUNT:
                break

            #Resort name and URL
            name_link = card.select_one("div.h3 a.h3")
            if not name_link:
                continue

            name = clean_text(name_link.get_text())
            resort_url = name_link["href"]

            #Country
            breadcrumbs = card.select("div.sub-breadcrumb a")
            country = breadcrumbs[1].get_text(strip=True) if len(breadcrumbs) > 1 else None

            #Elevation
            elevation = None
            elevation_icon = card.select_one("i.icon-uE002-height")
            if elevation_icon:
                elevation_td = elevation_icon.find_parent("td").find_next_sibling("td")
                elevation_text = clean_text(elevation_td.get_text())
                elevation = extract_number(elevation_text)

            #Slope info
            total_elem = card.select_one(".slopeinfoitem.active")
            blue_elem = card.select_one(".slopeinfoitem.blue")
            red_elem = card.select_one(".slopeinfoitem.red")
            black_elem = card.select_one(".slopeinfoitem.black")

            total_km = extract_number(total_elem.get_text()) if total_elem else None
            blue_km = extract_number(blue_elem.get_text()) if blue_elem else None
            red_km = extract_number(red_elem.get_text()) if red_elem else None
            black_km = extract_number(black_elem.get_text()) if black_elem else None

            #Lift price
            price_cell = card.select_one("i.icon-uE001-skipass")
            lift_price = None
            if price_cell:
                price_td = price_cell.find_parent("td").find_next_sibling("td")
                lift_price = clean_text(price_td.get_text())

            #Main town (from detail page)
            town = extract_main_town(resort_url)

            resorts.append({
                "resort_name": name,
                "country": country,
                "town": town,
                "elevation_m": elevation,
                "total_km": total_km,
                "blue_km": blue_km,
                "red_km": red_km,
                "black_km": black_km,
                "lift_price": lift_price
            })

        page += 1
        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(resorts)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nScraped {len(resorts)} resorts. Saved to {OUTPUT_FILE}")
    print(df.head())


if __name__ == "__main__":
    scrape_resort_list()
