import requests
import time
import json

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive"
}

BASE_URL = "https://www.nseindia.com/api/equity-stockIndices?index="

INDEXES = [
    "NIFTY 50",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP 50",
    "NIFTY MIDCAP 100",
    "NIFTY MIDCAP 150",
    "NIFTY TOTAL MARKET"
]

session = requests.Session()
session.headers.update(HEADERS)

def get_index_data(index_name, retries=3):
    encoded_index = index_name.replace(" ", "%20")
    url = BASE_URL + encoded_index

    for attempt in range(1, retries + 1):
        try:
            print(f"➡️ Fetching {index_name}… (attempt {attempt})")
            response = session.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            print(f"✅ Success: {index_name} ({len(data['data'])} stocks)\n")
            return data['data']
        except requests.exceptions.RequestException as e:
            print(f"  ❌ [{attempt}] Request error: {e}")
            time.sleep(3)  # Wait before retry

    print(f"   ⚠️ Skipping {index_name} (no valid JSON)\n")
    return []

def main():
    all_data = {}

    # First warm-up request to set NSE cookies
    try:
        session.get("https://www.nseindia.com", timeout=10)
    except Exception as e:
        print(f"❌ Error during warm-up requests: {e}")

    for index in INDEXES:
        data = get_index_data(index)
        all_data[index] = data
        time.sleep(1)  # avoid hammering server

    # Save data to file
    with open("nifty_data.json", "w") as f:
        json.dump(all_data, f, indent=2)

    print("✅ All data saved to 'nifty_data.json'")

if __name__ == "__main__":
    main()
