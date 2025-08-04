import requests
import pandas as pd
import os
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
    "Accept": "text/csv,application/csv"
}

def download_index_csv(index_name, code):
    url = f"https://www.nseindia.com/api/download-index?index={code}"
    s = requests.Session()
    s.headers.update(HEADERS)
    # fetch nseindia.com for cookies (required)
    s.get("https://www.nseindia.com", timeout=10)
    response = s.get(url, timeout=10)
    response.raise_for_status()
    filename = f"data/{index_name.replace(' ', '_')}.csv"
    with open(filename, "wb") as f:
        f.write(response.content)
    print(f"Downloaded {index_name} to {filename}")
    return filename

def main():
    os.makedirs("data", exist_ok=True)
    csv_files = []
    for index, code in INDEXES.items():
        try:
            fname = download_index_csv(index, code)
            csv_files.append(fname)
        except Exception as e:
            print(f"Failed to download {index}: {e}")
        time.sleep(1)

    # Consolidate CSVs
    dfs = []
    for fname in csv_files:
        df = pd.read_csv(fname)
        df.insert(0, "Index", fname.split('/')[-1].replace('.csv', '').replace('_', ' '))
        dfs.append(df)
    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        # Save as all_indices.csv, backup existing as old_data.csv if exists
        all_file = "data/all_indices.csv"
        old_file = "data/old_data.csv"
        if os.path.exists(all_file):
            os.replace(all_file, old_file)
        result.to_csv(all_file, index=False)
        print(f"Saved consolidated data to {all_file}")

if __name__ == "__main__":
    main()
