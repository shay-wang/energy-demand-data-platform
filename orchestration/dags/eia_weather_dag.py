from datetime import timedelta
import yaml
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

local_tz = pendulum.timezone("America/Chicago")
CONFIG_PATH = "/opt/airflow/ingestion/regions.yaml"


@dag(
    dag_id="energy_and_weather_daily",
    description="Extract EIA and Open-Meteo data to GCS, then load/transform in BQ.",
    start_date=pendulum.datetime(2026, 2, 10, tz=local_tz),
    schedule="30 4 * * *",  # Everyday at 4:30 am Central time
    catchup=False,
    tags=["eia", "weather", "ingestion", "bronze"],
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)

def energy_weather_pipeline():

    # -- SETUP TASK --
    @task
    def get_enabled_regions():
        """Reads YAML and returns a list of region dicts."""
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
        # Return only active regions
        return [r for r in config["regions"] if r.get("active", True)]

    # -- EXTRACTION TASKS (Parallel Dynamic Mapping) --

    # Fetch data from EIA API and move it to Landing (GCS)
    @task(task_id="extract_eia_to_gcs")
    def extract_eia_task(region_config, data_interval_end):
        """
        Run once per region to fetch T-3 settled data from EIA API and save as NDJSON in GCS Landing.
        """

        from ingestion.eia_ingest import fetch_single_region
        
        # T-3 Logic
        target_date = data_interval_end - timedelta(days=3)

        print(f"Fetching data for target date: {target_date.date()}")

        api_key = Variable.get("EIA_API_KEY")
        bucket_name = Variable.get("GCS_LANDING_BUCKET")

        # Handle connection to GSC bucket
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        client = hook.get_conn()

        fetch_single_region(api_key, region_config, target_date, bucket_name, client)
        return f"eia/year={target_date.year}/month={target_date.strftime('%m')}/day={target_date.strftime('%d')}/"

    # Fetch data from Open-Meteo API and move it to Landing (GCS)
    @task(task_id="extract_weather_to_gcs")
    def extract_weather_task(region_config, data_interval_end):
        """
        Run once per region to fetch T-3 settled data from Open-Meteo API and save as NDJSON in GCS Landing.
        """
        from ingestion.weather_ingest import fetch_weather_region

        # Match weather date to EIA data date (T-3)
        target_date = data_interval_end - timedelta(days=3)

        bucket_name = Variable.get("GCS_LANDING_BUCKET")
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        client = hook.get_conn()

        fetch_weather_region(region_config, target_date, bucket_name, client)
        return f"weather/year={target_date.year}/month={target_date.strftime('%m')}/day={target_date.strftime('%d')}/"

    # --- LOADING TASKS (BigQuery) ---
    # Move EIA data from Landing (GCS) to Bronze (BigQuery)
    load_eia_to_bronze = BigQueryInsertJobOperator(
        task_id="load_eia_to_bq_bronze",
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{Variable.get('GCS_LANDING_BUCKET')}/"
                    + "{{task_instance.xcom_pull(task_ids='extract_eia_to_gcs')[0] + '*.json' }}"
                ],
                "destinationTable": {
                    "projectId": "{{ var.value.GCP_PROJECT_ID }}",
                    "datasetId": "bronze",
                    "tableId": "eia_raw",
                },
                # Use ingestion time as the time partitioning field
                "timePartitioning": {"type": "DAY", "field": None},
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
                "hivePartitioningOptions": {
                    "mode": "AUTO",
                    "sourceUriPrefix": f"gs://{Variable.get('GCS_LANDING_BUCKET')}/eia/",
                    "requirePartitionFilter": False,
                },
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Move Weather data from Landing (GCS) to Bronze (BigQuery)
    load_weather_to_bronze = BigQueryInsertJobOperator(
        task_id="load_weather_to_bq_bronze",
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{Variable.get('GCS_LANDING_BUCKET')}/"
                    + "{{task_instance.xcom_pull(task_ids='extract_weather_to_gcs')[0] + '*.json' }}"
                ],
                "destinationTable": {
                    "projectId": "{{ var.value.GCP_PROJECT_ID }}",
                    "datasetId": "bronze",
                    "tableId": "weather_raw",
                },
                # Use ingestion time as the time partitioning field
                "timePartitioning": {"type": "DAY", "field": None},
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
                "hivePartitioningOptions": {
                    "mode": "AUTO",
                    "sourceUriPrefix": f"gs://{Variable.get('GCS_LANDING_BUCKET')}/weather/",
                    "requirePartitionFilter": False,
                },
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # --- ORCHESTRATION FLOW ---

    regions_list = get_enabled_regions()

    # Trigger Dynamic Mapping
    eia_extractions = extract_eia_task.expand(region_config=regions_list)
    weather_extractions = extract_weather_task.expand(region_config=regions_list)

    # Set Dependencies
    eia_extractions >> load_eia_to_bronze
    weather_extractions >> load_weather_to_bronze


energy_weather_pipeline()
