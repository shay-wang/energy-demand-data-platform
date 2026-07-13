import requests
import json
from datetime import datetime
import os
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def fetch_single_region(
    api_key, region_conf, target_date, bucket_name=None, client=None, local_path=None
):
    """
    Fetches data for ONE region and saves it either to GCS or Local Disk.
    """
    subba = region_conf["id"]
    logger.info(f"🚀 Starting ingestion for region: {subba} | Target Date: {target_date.strftime('%Y-%m-%d')}")
    start_time = time.time()

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

    try:
        logger.info(f"📡 Requesting EIA API data for subba={subba}...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()["response"]["data"]
        rows_fetched = len(data)

        if rows_fetched == 0:
            logger.warning(f"⚠️ API request successful, but 0 records returned for region {subba}.")
        else:
            logger.info(f"✅ Successfully fetched {rows_fetched} records from EIA.")

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ HTTP Request failed for region {subba}.")
        logger.error(f"Reason: {e.__class__.__name__} -> {str(e)}")
        sys.exit(1)
    
    # Convert to NDJSON
    ndjson_content = "\n".join([json.dumps(record) for record in data])

    # Define Hive-style path and file name. (Example: eia/year=2026/month=03/day=02/hourly_demand_NCEN.json)
    file_name = f"hourly_demand_{subba}.json"
    date_path = f"year={target_date.year}/month={target_date.strftime('%m')}/day={target_date.strftime('%d')}"

    # Save locally (for testing)
    if local_path:
        full_local_dir = os.path.join(local_path, "eia", date_path)
        os.makedirs(full_local_dir, exist_ok=True)
        target_file = os.path.join(full_local_dir, file_name)
        with open(target_file, "w") as f:
            f.write(ndjson_content)
        f"💾 Local Write: Saved {rows_fetched} rows to {target_file}"

    # Upload to GCS (For Production)
    if client and bucket_name:
        gcs_path = f"eia/{date_path}/{file_name}"
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)

        logger.info(f"💾 Streaming upload initiated for gs://{bucket_name}/{gcs_path}...")
        blob.upload_from_string(ndjson_content, content_type="application/x-ndjson")
        logger.info(f"📥 GCS Upload Success: Wrote {rows_fetched} rows to gs://{bucket_name}/{gcs_path}")

    duration = round(time.time() - start_time, 2)
    logger.info(f"🏁 Finished processing region: {subba} in {duration} seconds.")
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

    fetch_single_region(
        api_key=os.getenv("EIA_API_KEY"),
        region_conf=config['regions'][0],
        target_date=datetime(2025, 12, 4),
        local_path=PROJECT_ROOT / "data"
    )

    # fetch_single_region(
    #     api_key=os.getenv("EIA_API_KEY"),
    #     region_conf=config["regions"][0],
    #     target_date=datetime(2026, 2, 12),
    #     client=client,
    #     bucket_name=bucket_name,
    # )
