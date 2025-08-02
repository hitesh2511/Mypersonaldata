import os
import requests
import pandas as pd
from datetime import datetime
import time

INDEXES = {
    "NIFTY 50": "NIFTY%2050",
    "NIFTY NEXT 50": "NIFTY%20NEXT%2050",
    "NIFTY MIDCAP 150": "NIFTY%20MIDCAP%20150",
    "NIFTY MIDCAP 50": "NIFTY%20MIDCAP%2050",
    "NIFTY MIDCAP 100": "NIFTY%20MIDCAP%20100",
    "NIFTY TOTAL MARKET": "NIFTY%20TOTAL%20MARKET"
}

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
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

def safe_get_json(session, url, retries=3, backoff=2):
    """GET url up to `retries` times, return parsed JSON or None."""
    for attempt in range(1, retries+1):
        resp = session.get(url, timeout=10)
        # 1) Must be HTTP 200
        if resp.status_code != 200:
            print(f"  ❌ [{attempt}] {url} → HTTP {resp.status_code}")
        # 2) Content-Type should be JSON
        ct = resp.headers.get("Content-Type","")
        if "application/json" not in ct:
            snippet = resp.text.strip().replace("\n"," ")[:200]
            print(f"  ❌ [{attempt}] Not JSON (Content-Type: {ct}). Snippet: {snippet!r}")
        else:
            # 3) Try parsing JSON
            try:
                return resp.json()
            except Exception as e:
                print(f"  ❌ [{attempt}] JSON parse error: {e}")
        time.sleep(backoff)
    return None

def fetch_all_indices():
    session = requests.Session()
    session.headers.update(HEADERS)

    # Warm-up to get cookies
    session.get("https://www.nseindia.com", timeout=10)
    session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
    time.sleep(2)

    combined = []

    for name, code in INDEXES.items():
        print(f"➡️ Fetching {name}…")
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={code}"
        payload = safe_get_json(session, url)
        if not payload or "data" not in payload:
            print(f"   ⚠️ Skipping {name} (no valid JSON)")
            continue

        data = payload["data"]
        if not data:
            print(f"   ⚠️ No rows for {name}")
            continue

        df = pd.DataFrame(data)
        df.insert(0, "Index", name)
        combined.append(df)
        time.sleep(2)

    if not combined:
        print("❌ No data fetched for any index.")
        return

    result = pd.concat(combined, ignore_index=True)
    result.drop_duplicates(subset="symbol", inplace=True)

    os.makedirs("data", exist_ok=True)
    filename = "data/all_indices.csv"
    old_filename = "data/old_data.csv"

    # Move existing all_indices.csv to old_data.csv before saving new file
    if os.path.exists(filename):
        os.replace(filename, old_filename)
        print(f"♻️ Moved previous data to {old_filename}")

    result.to_csv(filename, index=False)
    print(f"✅ Combined data saved as {filename}")

if __name__ == "__main__":
    fetch_all_indices()
