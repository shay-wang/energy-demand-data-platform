from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

local_tz = pendulum.timezone("America/Chicago")


@dag(
    dag_id="ingest_eia_energy",
    start_date=pendulum.datetime(2026, 2, 10, tz=local_tz),
    schedule="30 4 * * *",
    catchup=True,
    tags=["eia", "ingestion"],
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def eia_ingestion_dag():
    @task(task_id="fetch_and_store_eia_data")
    def fetch_settled_data(logical_date):
        from ingestion.eia_ingest import fetch_eia_data

        target_date = (logical_date - timedelta(days=2)).date()
        print(
            f"Executing for logical date {logical_date.date()}, "
            f"but fetching settled data for {target_date}"
        )

        api_key = Variable.get("EIA_API_KEY")
        bucket_name = Variable.get("GCS_LANDING_BUCKET")
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        client = hook.get_conn()
        fetch_eia_data(api_key, target_date, bucket_name, client)

    fetch_settled_data()


eia_ingestion_dag()
