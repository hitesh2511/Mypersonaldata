import requests
import pandas as pd
from datetime import datetime
import time

# Indexes to fetch (friendly name → URL code)
INDEXES = {
    "NIFTY 50":        "NIFTY%2050",
    "NIFTY NEXT 50":   "NIFTY%20NEXT%2050",
    "NIFTY MIDCAP 150":"NIFTY%20MIDCAP%20150",
    "NIFTY MIDCAP 50" : "NIFTY%20MIDCAP%2050",
    "NIFTY MIDCAP 100" : "NIFTY%20MIDCAP%20100",
    "NIFTY TOTAL MARKET":   "NIFTY%20TOTAL%20MARKET"
}

# Browser‑style headers
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

def fetch_all_indices():
    session = requests.Session()
    session.headers.update(HEADERS)

    # Warm‑up requests to set cookies
    session.get("https://www.nseindia.com", timeout=10)
    session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
    time.sleep(2)

    today = datetime.now().strftime("%Y-%m-%d")
    combined = []

    for name, code in INDEXES.items():
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={code}"
        resp = session.get(url, timeout=10)
        resp.raise_for_status()

        data = resp.json().get("data", [])
        if not data:
            print(f"⚠️ No data for {name}")
            continue

        df = pd.DataFrame(data)
        df.insert(0, "Index", name)  # add an 'Index' column at front
        combined.append(df)

        time.sleep(2)  # polite pause

    if not combined:
        print("❌ No data fetched for any index.")
        return

    result = pd.concat(combined, ignore_index=True)

    # 🔻 Remove duplicates based on 'symbol' column
    result.drop_duplicates(subset="symbol", inplace=True)

    filename = f"all_indices_{today}.csv"
    result.to_csv(filename, index=False)
    print(f"✅ Combined data saved as {filename}")

if __name__ == "__main__":
    fetch_all_indices()
