import os
import json
import requests
from datetime import datetime
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

def fetch_weather_region(
    region_conf, target_date: datetime, bucket_name=None, client=None, local_path=None
):
    """
    Fetches weather for ONE region's coordinates and stamps it with the Subba ID.
    """
    subba = region_conf["id"]
    lat = region_conf["lat"]
    lon = region_conf["lon"]
    logger.info(f"🚀 Starting ingestion for region: {subba} | Target Date: {target_date.strftime('%Y-%m-%d')}")
    start_time = time.time()
    
    # Fetch data from Open-Meteo API URL, requesting temperature, humidity, feels like, and wind speed
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": target_date.strftime("%Y-%m-%d"),
        "end_date": target_date.strftime("%Y-%m-%d"),
        "hourly": "temperature_2m,relative_humidity_2m,apparent_temperature,wind_speed_10m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "timezone": "UTC",
    }
    
    try:
        logger.info(f"📡 Requesting Open-Meteo API data for subba={subba}...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        rows_fetched = len(res_json["hourly"]["time"])
        
        if rows_fetched == 0:
            logger.warning(f"⚠️ API request successful, but 0 records returned for region {subba}.")
        else:
            logger.info(f"✅ Successfully fetched {rows_fetched} records from Open-Meteo.")

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ HTTP Request failed for region {subba}.")
        logger.error(f"Reason: {e.__class__.__name__} -> {str(e)}")
        sys.exit(1)

    # Convert to NDJSON
    actual_lat = res_json.get("latitude")
    actual_lon = res_json.get("longitude")
    hourly_data = res_json["hourly"]
    records = []

    for i in range(len(hourly_data["time"])):
        record = {
            "timestamp_utc": hourly_data["time"][i],
            "subba": subba,  # add region join key
            "request_lat": lat,  # lat used for fetching (from YAML)
            "request_lon": lon,  # lon used for fetching (from YAML)
            "actual_lat": actual_lat,  # lat returned from API
            "actual_lon": actual_lon,  # lon returned from API
            "temp_f": hourly_data["temperature_2m"][i],
            "humidity_pct": hourly_data["relative_humidity_2m"][i],
            "apparent_temp_f": hourly_data["apparent_temperature"][i],
            "wind_speed_mph": hourly_data["wind_speed_10m"][i],
        }
        records.append(json.dumps(record))

    ndjson_content = "\n".join(records)

    # Define Hive-style path and file name. (Example: weather/year=2026/month=03/day=02/weather_NCEN.json)
    file_name = f"weather_{subba}.json"
    date_path = f"year={target_date.year}/month={target_date.strftime('%m')}/day={target_date.strftime('%d')}"

    # Save Locally (For Testing)
    if local_path:
        full_local_dir = os.path.join(local_path, "weather", date_path)
        os.makedirs(full_local_dir, exist_ok=True)
        target_file = os.path.join(full_local_dir, file_name)
        with open(target_file, "w") as f:
            f.write(ndjson_content)
        f"💾 Local Write: Saved {rows_fetched} rows to {target_file}"

    # Upload to GCS (For Production)
    if client and bucket_name:
        gcs_path = f"weather/{date_path}/{file_name}"
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)

        logger.info(f"💾 Streaming upload initiated for gs://{bucket_name}/{gcs_path}...")
        blob.upload_from_string(ndjson_content, content_type="application/x-ndjson")
        logger.info(f"📥 GCS Upload Success: Wrote {rows_fetched} rows to gs://{bucket_name}/{gcs_path}")

    duration = round(time.time() - start_time, 2)
    logger.info(f"🏁 Finished processing region: {subba} in {duration} seconds.")
    return records


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

    fetch_weather_region(
        region_conf=config['regions'][0],
        target_date=datetime(2026, 2, 10),
        local_path=PROJECT_ROOT / "data"
    )

    # fetch_weather_region(
    #     region_conf=config["regions"][0],
    #     target_date=datetime(2026, 2, 12),
    #     client=client,
    #     bucket_name=bucket_name,
    # )
