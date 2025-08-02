#!/usr/bin/env python3

from nsepython import nsefetch
import pandas as pd
from datetime import datetime
import os

# 1) The exact “dropdown” names NSE expects:
INDEXES = {
    "NIFTY 50":           "Nifty 50",
    "NIFTY NEXT 50":      "Nifty Next 50",
    "NIFTY MIDCAP 150":   "Nifty Midcap 150",
    "NIFTY MIDCAP 50":    "Nifty Midcap 50",
    "NIFTY MIDCAP 100":   "Nifty Midcap 100",
    "NIFTY TOTAL MARKET": "Nifty Total Market"
}

def fetch_all_indices():
    all_frames = []
    for name, idx in INDEXES.items():
        print(f"➡️ Fetching {name}…")
        try:
            payload = nsefetch(
                f"https://www.nseindia.com/api/equity-stockIndices?index={idx}"
            )
            data = payload.get("data", [])
            if not data:
                print(f"   ⚠️ No data for {name}")
                continue

            df = pd.DataFrame(data)
            df.insert(0, "Index", name)
            all_frames.append(df)
        except Exception as e:
            print(f"   ❌ Failed {name}: {e}")

    if not all_frames:
        print("❌ No data fetched for any index.")
        return

    result = pd.concat(all_frames, ignore_index=True)
    result.drop_duplicates(subset="symbol", inplace=True)

    # 2) Save to a predictable file path
    os.makedirs("data", exist_ok=True)
    filename = f"data/all_indices_{datetime.now():%Y-%m-%d}.csv"
    result.to_csv(filename, index=False)
    print(f"✅ Combined data saved as {filename}")

if __name__ == "__main__":
    fetch_all_indices()
