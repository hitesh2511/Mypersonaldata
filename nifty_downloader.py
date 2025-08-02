#!/usr/bin/env python3
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select

# 1) The indexes you want to scrape
INDEXES = [
    "NIFTY 50",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP 150",
    "NIFTY MIDCAP 50",
    "NIFTY MIDCAP 100",
    "NIFTY TOTAL MARKET"
]

URL = "https://www.nseindia.com/market-data/live-equity-market"

def fetch_all_indices():
    # 2) Configure headless Chrome
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=opts)

    driver.get(URL)
    time.sleep(5)  # let the page and JS load

    combined = []
    for idx_name in INDEXES:
        # 3) Select the dropdown
        sel = Select(driver.find_element("id", "equitySegmentIndex"))
        sel.select_by_visible_text(idx_name)
        time.sleep(5)  # wait for the table to refresh

        # 4) Read the rendered table into pandas
        table = driver.find_element("xpath", "//table")
        df = pd.read_html(table.get_attribute("outerHTML"))[0]
        df.insert(0, "Index", idx_name)
        combined.append(df)

    driver.quit()

    # 5) Combine, dedupe, and save
    all_df = pd.concat(combined, ignore_index=True)
    all_df.drop_duplicates(subset="Symbol", inplace=True)
    filename = f"data/all_indices_{datetime.now():%Y-%m-%d}.csv"
    all_df.to_csv(filename, index=False)
    print(f"✅ Saved {filename}")

if __name__ == "__main__":
    fetch_all_indices()
