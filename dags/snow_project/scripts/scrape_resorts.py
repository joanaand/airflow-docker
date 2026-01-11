#Scrape ski resort list from skiresort.info
#Extract: Name, Country, URL, Total km, Blue/Red/Black km, Lift price, Town, Elevation

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from pathlib import Path
from airflow.models import Variable


#Get base URL from Airflow Variable (with fallback)
BASE_LIST_URL = Variable.get(
    "SKI_RESORT_LIST_URL",
    default_var="https://www.skiresort.info/best-ski-resorts/"
)

PAGE_URL = BASE_LIST_URL.rstrip("/") + "/page/{page}/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataEngineeringBot/1.0)"
}

RESORT_COUNT = 100
REQUEST_TIMEOUT = 10
REQUEST_DELAY = 2

#Path handling
BASE_DIR = Path(__file__).resolve().parents[1]
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

            name = clean_text(name_link
