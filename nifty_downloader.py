from nsepython import nsefetch
import pandas as pd
from datetime import datetime
import os

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
        payload = nsefetch(f"https://www.nseindia.com/api/equity-stockIndices?index={idx}")
        df = pd.DataFrame(payload.get("data", []))
        if not df.empty:
            df.insert(0, "Index", name)
            all_frames.append(df)
        else:
            print(f"   ⚠️ No data for {name}")

    if not all_frames:
        print("❌ No indices fetched.")
        return

    result = pd.concat(all_frames, ignore_index=True)
    result.drop_duplicates(subset="symbol", inplace=True)

    os.makedirs("data", exist_ok=True)
    filename = f"data/all_indices_{datetime.now():%Y-%m-%d}.csv"
    result.to_csv(filename, index=False)
    print(f"✅ Saved {filename}")

if __name__ == "__main__":
    fetch_all_indices()
