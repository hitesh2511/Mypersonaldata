import requests
import pandas as pd
from datetime import datetime
import time

INDEXES = {
    "NIFTY 50":           "NIFTY%2050",
    "NIFTY NEXT 50":      "NIFTY%20NEXT%2050",
    "NIFTY MIDCAP 150":   "NIFTY%20MIDCAP%20150",
    "NIFTY MIDCAP 50":    "NIFTY%20MIDCAP%2050",
    "NIFTY MIDCAP 100":   "NIFTY%20MIDCAP%20100",
    "NIFTY TOTAL MARKET": "NIFTY%20TOTAL%20MARKET"
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
    "Host": "www.nseindia.com",
    "Origin": "https://www.nseindia.com"
}

def safe_get_json(session, url, retries=3, backoff=2):
    for attempt in range(1, retries + 1):
        resp = session.get(url, timeout=10)
        ct = resp.headers.get("Content-Type", "")
        if resp.status_code == 200 and "application/json" in ct:
            try:
                return resp.json()
            except Exception as e:
                print(f"  ❌ [{attempt}] JSON parse error: {e}")
        else:
            print(f"  ❌ [{attempt}] {url} → HTTP {resp.status_code} or Invalid Content-Type")
        time.sleep(backoff)
    return None

def fetch_all_indices():
    session = requests.Session()
    session.headers.update(HEADERS)

    # Warm-up requests to generate cookies
    try:
        session.get("https://www.nseindia.com", timeout=10)
        session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
    except Exception as e:
        print(f"Error warming up session: {e}")
        return

    time.sleep(2)

    today = datetime.now().strftime("%Y-%m-%d")
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
    filename = f"all_indices_{today}.csv"
    result.to_csv(filename, index=False)
    print(f"✅ Combined data saved as {filename}")

if __name__ == "__main__":
    fetch_all_indices()
