import requests
import json
from datetime import datetime
import os


def fetch_single_region(
    api_key, region_conf, target_date, bucket_name=None, client=None, local_path=None
):
    """
    Fetches data for ONE region and saves it either to GCS or Local Disk.
    """
    subba = region_conf["id"]

    # Fetch data
    url = "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"
    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[subba][]": subba,
        "start": target_date.strftime("%Y-%m-%dT00"),
        "end": target_date.strftime("%Y-%m-%dT23"),
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()["response"]["data"]

    # Convert to NDJSON
    ndjson_content = "\n".join([json.dumps(record) for record in data])

    # Define Hive-style path and file name. (Example: eia/year=2026/month=03/day=02/hourly_demand_NCEN.json)
    file_name = f"hourly_demand_{subba}.json"
    date_path = f"year={target_date.year}/month={target_date.strftime('%m')}/day={target_date.strftime('%d')}"

    # Save locally (for testing)
    if local_path:
        full_local_dir = os.path.join(local_path, "eia", date_path)
        os.makedirs(full_local_dir, exist_ok=True)
        with open(os.path.join(full_local_dir, file_name), "w") as f:
            f.write(ndjson_content)
        print(f"Saved locally to {full_local_dir}/{file_name}")

    # Upload to GCS (For Production)
    if client and bucket_name:
        gcs_path = f"eia/{date_path}/{file_name}"
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(ndjson_content, content_type="application/x-ndjson")
        print(f"Successfully uploaded to gs://{bucket_name}/{gcs_path}")

    return data


if __name__ == "__main__":
    import yaml
    from pathlib import Path
    from google.cloud import storage
    from google.oauth2 import service_account
    from dotenv import load_dotenv

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    load_dotenv(PROJECT_ROOT / ".env")

    creds_path = os.getenv("INGEST_SA_KEY_PATH")
    abs_path = PROJECT_ROOT / creds_path

    creds = service_account.Credentials.from_service_account_file(abs_path)
    client = storage.Client(credentials=creds, project=os.getenv("GCP_PROJECT_ID"))

    bucket_name = os.getenv("GCS_LANDING_BUCKET")

    with open("ingestion/regions.yaml", "r") as f:
        config = yaml.safe_load(f)

    # fetch_single_region(
    #     api_key=os.getenv("EIA_API_KEY"),
    #     region_conf=config['regions'][0],
    #     target_date=datetime(2026, 2, 10),
    #     local_path=PROJECT_ROOT / "data"
    # )

    fetch_single_region(
        api_key=os.getenv("EIA_API_KEY"),
        region_conf=config["regions"][0],
        target_date=datetime(2026, 2, 12),
        client=client,
        bucket_name=bucket_name,
    )
