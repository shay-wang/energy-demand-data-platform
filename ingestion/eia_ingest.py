import requests
import json
from datetime import date
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
import pendulum


def fetch_eia_data(api_key, target_date: date, bucket_name: str, client):
    # Check if the API key is set
    if not api_key:
        raise RuntimeError("EIA_API_KEY not set")

    # Setup timezone as US Central and get the correct query hour range for a target date
    local_tz = pendulum.timezone("America/Chicago")
    start_dt = pendulum.datetime(
        target_date.year, target_date.month, target_date.day, tz=local_tz
    )
    end_dt = start_dt.end_of("day")
    start_str = start_dt.format("YYYY-MM-DDTHH") + start_dt.format("ZZ")[:3]
    end_str = end_dt.format("YYYY-MM-DDTHH") + end_dt.format("ZZ")[:3]

    # Fetch data
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
    raw_response = response.json()
    records = raw_response.get("response", {}).get("data", [])

    if not records:
        print("No data returned from API. Skipping write.")
        return

    # Convert to NDJSON format
    ndjson_data = "\n".join([json.dumps(record) for record in records])

    # Set up Hive-style path. (Example: eia/year=2026/month=03/day=02/)
    path_parts = [
        "eia",
        f"year={target_date.year}",
        f"month={target_date.strftime('%m')}",
        f"day={target_date.strftime('%d')}",
    ]
    folder_path = "/".join(path_parts)
    filename = f"{folder_path}/local_hourly_demand.json"

    # Load data to GCS landing bucket
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)

    blob.upload_from_string(
        data= ndjson_data,
        content_type="application/x-ndjson"
    )

    print(f"Successfully uploaded to gs://{bucket_name}/{filename}")


if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    load_dotenv(PROJECT_ROOT / ".env")

    creds_path = os.getenv("INGEST_SA_KEY_PATH")
    abs_path = PROJECT_ROOT / creds_path

    creds = service_account.Credentials.from_service_account_file(abs_path)
    client = storage.Client(credentials=creds, project=os.getenv("GCP_PROJECT_ID"))

    api_key = os.getenv("EIA_API_KEY")
    target_date = date.fromisoformat("2026-02-11")
    bucket_name = os.getenv("GCS_LANDING_BUCKET")
    fetch_eia_data(api_key, target_date, bucket_name, client)
