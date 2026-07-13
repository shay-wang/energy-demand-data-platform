# How to Run

## Prerequisites
- Docker & Docker Compose
- Google Cloud SDK (authenticated with owner/editor access to target GCP project)
- Terraform v1.5.0+
- An [EIA API key](https://www.eia.gov/opendata/documentation.php)

## Credentials
Create a local `.env` file for API credentials and project configuration inside the `orchestration` folder:
```
# orchestration/.env
EIA_API_KEY=
GCP_PROJECT_ID=
GCS_LANDING_BUCKET="${GCP_PROJECT_ID}-landing"
```
Place your two generated GCP Service Account JSON keys inside the `.creds` directory at the project root.
``` bash
project-root/
└── .creds/
    ├── ingest-sa-key.json   (Roles: Storage Object Admin, BigQuery Data Editor, BigQuery Job User)
    └── dbt-sa-key.json      (Roles: BigQuery Data Viewer, BigQuery Data Editor, BigQuery Job User)
```
## Provision Cloud Resources
Open `infra/terraform/variables.tf` and update the default value for your GCP project ID.

Initialize and apply the Terraform configuration:
  ```bash
  cd infra/terraform
  terraform init
  terraform plan
  terraform apply
  ```

## Execution Flow Using Airflow

### 1. Launch the Platform
Navigate to the orchestration directory and start the container stack. 
```bash
cd ../orchestration
docker compose up -d
```
- Airflow UI: http://localhost:8080
- Credentials: `admin`/`admin`

### 2. Pipeline Execution
Unpause and trigger `energy_and_weather_daily` DAG manually in the UI to run the active processing schedule.

### 3. Historical Replays (Optional CLI)
To backfill processing for a historical window, run the native Airflow utility directly inside the active scheduler container:

```bash
docker compose exec airflow-worker airflow dags backfill \
    --start-date 2026-03-01 \
    --end-date 2026-03-31 \
    energy_and_weather_daily
```
