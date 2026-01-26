from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


from scripts.eia_ingest import fetch_eia_data, save_raw_data


def run_eia_ingestion():
    data = fetch_eia_data()
    save_raw_data(data)


with DAG(
    dag_id="ingest_eia_energy",
    start_date=datetime(2026,1,1),
    schedule_interval="@daily",
    catchup=False,
    tags=["eia", "ingestion",]
) as dag:
    
    ingest_task = PythonOperator(
        task_id="fetch_and_store_eia_data",
        python_callable=run_eia_ingestion,
    )

    ingest_task