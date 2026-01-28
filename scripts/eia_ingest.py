import requests
import json
from datetime import timedelta, date
from pathlib import Path
from dotenv import load_dotenv
import os
import re

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "eia"
DATA_DELAY_DAYS = 2


def fetch_eia_data(api_key: str, start_date: date, end_date: date):
    url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "facets[respondent][]": "ERCO",
        "facets[timezone][]": "Central",
        "data[0]": "value",
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def save_raw_data(data, output_dir: Path, start_date: date, end_date: date):
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = (
        output_dir
        / f"eia_energy_{start_date.isoformat()}_to_{end_date.isoformat()}.json"
    )
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)


def get_last_ingested_date(raw_dir: Path) -> date | None:
    if not raw_dir.exists():
        return None

    pattern = re.compile(r"eia_energy_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.json")
    dates = []

    for file in raw_dir.glob("eia_energy_*.json"):
        match = pattern.match(file.name)
        if match:
            dates.append(date.fromisoformat(match.group(2)))

    return max(dates) if dates else None


if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise RuntimeError("EIA_API_KEY not set")

    last_date = get_last_ingested_date(RAW_DIR)
    start_date = last_date + timedelta(days=1) if last_date else date(2026, 1, 1)
    end_date = date.today() - timedelta(days=DATA_DELAY_DAYS)

    if start_date > end_date:
        print("No new data to ingest.")
    else:
        data = fetch_eia_data(api_key, start_date, end_date)
        if not data.get("response", {}).get("data"):
            print("No data returned from API. Skipping write.")
        else:
            save_raw_data(data, RAW_DIR, start_date, end_date)
