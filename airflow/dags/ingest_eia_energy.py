from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from scripts.eia_ingest import run_incremental_ingestion


with DAG(
    dag_id="ingest_eia_energy",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=[
        "eia",
        "ingestion",
    ],
) as dag:

    ingest_task = PythonOperator(
        task_id="fetch_and_store_eia_data",
        python_callable=run_incremental_ingestion,
        op_kwargs={"api_key": Variable.get("EIA_API_KEY")},
    )

    ingest_task
