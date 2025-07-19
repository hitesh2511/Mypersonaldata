from nsepython import nsefetch
import pandas as pd
from datetime import datetime

INDEXES = {
    "NIFTY 50":          "Nifty 50",
    "NIFTY NEXT 50":     "Nifty Next 50",
    "NIFTY MIDCAP 150":  "Nifty Midcap 150",
    "NIFTY MIDCAP 50":   "Nifty Midcap 50",
    "NIFTY MIDCAP 100":  "Nifty Midcap 100",
    "NIFTY TOTAL MARKET":"Nifty Total Market"
}

combined = []
for name, idx in INDEXES.items():
    payload = nsefetch(f"https://www.nseindia.com/api/equity-stockIndices?index={idx}")
    df = pd.DataFrame(payload["data"])
    df.insert(0, "Index", name)
    combined.append(df)

all_df = pd.concat(combined, ignore_index=True).drop_duplicates(subset="symbol")
all_df.to_csv(f"all_indices_{datetime.now():%Y-%m-%d}.csv", index=False)
