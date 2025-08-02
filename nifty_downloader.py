#!/usr/bin/env python3
import requests
import pandas as pd
from datetime import datetime
import time

# Configuration
API_BASE = "https://www.nseindia.com/api/equity-stockIndices?index="
INDEXES = [
    "NIFTY 50",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP 150",
    "NIFTY MIDCAP 50",
    "NIFTY MIDCAP 100",
    "NIFTY TOTAL MARKET"
]
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/live-equity-market",
    "Origin": "https://www.nseindia.com",
    "Connection": "keep-alive",
}

def get_index_df(name: str, session: requests.Session, retries=3) -> pd.DataFrame:
    url = API_BASE + name.replace(" ", "%20")
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            df = pd.DataFrame(data)
            df.insert(0, "Index", name)
            return df
        except Exception as e:
            print(f"  ❌ [{attempt}] Failed {name}: {e}")
            time.sleep(2)
    print(f"⚠️ Skipping {name}")
    return pd.DataFrame()

def main():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get("https://www.nseindia.com", timeout=5)
    session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=5)
    time.sleep(2)

    all_frames = []
    today = datetime.now().strftime("%Y-%m-%d")

    for idx in INDEXES:
        print(f"➡️ Fetching {idx}…")
        df = get_index_df(idx, session)
        if not df.empty:
            all_frames.append(df)
        time.sleep(2)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        combined.drop_duplicates(subset="symbol", inplace=True)
        filename = f"all_indices_{today}.csv"
        combined.to_csv(filename, index=False)
        print(f"✅ Saved {filename}")
    else:
        print("❌ No data fetched for any index.")

if __name__ == "__main__":
    main()
