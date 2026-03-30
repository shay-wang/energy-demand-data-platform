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
    dag_id="ext_eia_gcs_to_bq_daily",
    description="Extract EIA to GCS, then load/transform in BQ.",
    start_date=pendulum.datetime(2026, 2, 10, tz=local_tz),
    schedule="30 4 * * *",  # Everyday at 4:30 am Central time
    catchup=False,
    tags=["eia", "daily", "ingestion", "bronze"],
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def eia_pipeline():
    # Get region list from the yaml config file
    @task
    def get_enabled_regions():
        """Reads YAML and returns a list of region dicts."""
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
        # Return only active regions
        return [r for r in config["regions"] if r.get("active", True)]

    # Fetch regional hourly demand data from EIA API and move it to Landing (GCS)
    @task(task_id="extract_eia_api_to_gcs_raw")
    def extract_to_gcs(region_config, logical_date):
        """
        Run once per region to fetch T-2 settled data from EIA API and saves as NDJSON in GCS Landing.
        """
        from ingestion.eia_ingest import fetch_single_region

        # T-2 Logic
        target_date = logical_date - timedelta(days=3)

        api_key = Variable.get("EIA_API_KEY")
        bucket_name = Variable.get("GCS_LANDING_BUCKET")

        # Handle connection to GSC bucket
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        client = hook.get_conn()

        fetch_single_region(api_key, region_config, target_date, bucket_name, client)
        return f"eia/year={target_date.year}/month={target_date.strftime('%m')}/day={target_date.strftime('%d')}/"

    # Move data from Landing (GCS) to Bronze (BigQuery)
    load_raw_to_bronze_table = BigQueryInsertJobOperator(
        task_id="load_gcs_to_bq_bronze",
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{Variable.get('GCS_LANDING_BUCKET')}/"
                    + "{{task_instance.xcom_pull(task_ids='extract_eia_api_to_gcs_raw')[0] + '*.json' }}"
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

    regions_list = get_enabled_regions()
    extract_task = extract_to_gcs.expand(region_config=regions_list)

    extract_task >> load_raw_to_bronze_table


eia_pipeline()
