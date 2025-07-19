import requests
import pandas as pd
from datetime import datetime

URL = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.nseindia.com/"
}

def fetch_nifty_data():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get("https://www.nseindia.com", timeout=5)  # Get cookies

    resp = session.get(URL, timeout=10)
    data = resp.json().get("data", [])

    df = pd.DataFrame(data)
    today = datetime.now().strftime("%Y-%m-%d")
    df.to_csv(f"nifty_data_{today}.csv", index=False)
    print(f"✅ Data saved as nifty_data_{today}.csv")

if __name__ == "__main__":
    fetch_nifty_data()