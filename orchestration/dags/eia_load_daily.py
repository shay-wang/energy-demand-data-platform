from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

local_tz = pendulum.timezone("America/Chicago")


@dag(
    dag_id="ext_eia_gcs_to_bq_daily",
    description="Extract EIA to GCS, then load/transform in BQ.",
    start_date=pendulum.datetime(2026, 2, 10, tz=local_tz),
    schedule="30 4 * * *",
    catchup=False,
    tags=["eia", "daily", "ingestion", "bronze"],
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def eia_pipeline():
    @task(task_id="extract_eia_api_to_gcs_raw")
    def extract_to_gcs(logical_date):
        """
        Fetches T-2 settled data from EIA API and saves as NDJSON in GCS Landing.
        """
        from ingestion.eia_ingest import fetch_eia_data

        # T-2 Logic
        target_date = (logical_date - timedelta(days=2)).date()

        api_key = Variable.get("EIA_API_KEY")
        bucket_name = Variable.get("GCS_LANDING_BUCKET")

        # Handle connection to GSC bucket
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        client = hook.get_conn()

        fetch_eia_data(api_key, target_date, bucket_name, client)
        return f"eia/year={target_date.year}/month={target_date.strftime('%m')}/day={target_date.strftime('%d')}/"

    # Moving data from Landing (GCS) to Bronze (BigQuery)
    load_raw_to_bronze_table = BigQueryInsertJobOperator(
        task_id="load_gcs_to_bq_bronze",
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{Variable.get('GCS_LANDING_BUCKET')}/"
                    + "{{task_instance.xcom_pull(task_ids='extract_eia_api_to_gcs_raw') + '*.json' }}"
                ],
                "destinationTable": {
                    "projectId": "{{ var.value.GCP_PROJECT_ID }}",
                    "datasetId": "bronze",
                    "tableId": "eia_raw",
                },
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

    extract_task = extract_to_gcs()
    extract_task >> load_raw_to_bronze_table


eia_pipeline()
