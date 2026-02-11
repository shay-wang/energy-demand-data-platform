import requests
import json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()
EIA_API_KEY = os.getenv("EIA_API_KEY")
PROJECT_ROOT = Path(__file__).resolve().parents[1]

def fetch_eia_data():
    url = " https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"
    params = {
    "api_key": EIA_API_KEY,
    "frequency": "daily",
    "facets[respondent][]": "ERCO",
    "facets[timezone][]": "Central",
    "data[0]": "value",
    "start": "2023-01-02",
    "end": "2023-01-05"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def save_raw_data(data):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = PROJECT_ROOT/"data"/"raw"/"eia"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"eia_energy_{today}.json"
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

if __name__ == "__main__":
    data = fetch_eia_data()
    save_raw_data(data)
