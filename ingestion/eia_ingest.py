import requests
import json
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
import os
import pendulum

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = PROJECT_ROOT / "data" / "raw" / "eia"


def fetch_eia_data(api_key, target_date: date):
    if not api_key:
        raise RuntimeError("EIA_API_KEY not set")
    local_tz = pendulum.timezone("America/Chicago")
    start_dt = pendulum.datetime(
        target_date.year, target_date.month, target_date.day, tz=local_tz
    )
    end_dt = start_dt.end_of("day")
    start_str = start_dt.format("YYYY-MM-DDTHH") + start_dt.format("ZZ")[:3]
    end_str = end_dt.format("YYYY-MM-DDTHH") + end_dt.format("ZZ")[:3]
    url = " https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"
    params = {
        "api_key": api_key,
        "frequency": "local-hourly",
        "data[0]": "value",
        "facets[parent][]": "ERCO",
        "facets[subba][]": "NCEN",
        "start": start_str,
        "end": end_str,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if not data.get("response", {}).get("data"):
        print("No data returned from API. Skipping write.")
        return
    filename = f"eia_energy_{target_date.isoformat()}.json"
    output_dir = BASE_DIR
    output_file_path = output_dir / filename
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_file_path, "w") as f:
        json.dump(data, f, indent=2)
    print(
        f"Saved {len(data.get('response', {}).get('data', []))} records to {output_file_path}"
    )


if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("EIA_API_KEY")
    target_date = date.fromisoformat("2026-02-01")
    fetch_eia_data(api_key, target_date)
