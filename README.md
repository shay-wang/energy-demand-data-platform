# Energy Demand Data Platform

This project ingests public energy demand data and processes it into analytics-ready datasets using Airflow and Python. This project is designed to demonstrate end-to-end data engineering skills, including data ingestion, orchestration, and data layer design, using realistic tools and workflows.

## Data Source
- U.S. Energy Information Administration (EIA) API
  - Public energy demand and consumption datasets

## Architecture (High-level)
- Python scripts for data ingestion logic
- Apache Airflow for workflow orchestration
- Local Docker-based development environment
- Logical data layers documented in-repo

## Scope

This repository focuses on ingestion and orchestration.
Transformation, analytics modeling, and serving layers will be added incrementally as the project evolves.

## Airflow Local Setup (Docker)

### 1. Initialize the database and create an admin user (first run only)
```bash
cd airflow

docker compose run --rm airflow-webserver airflow db migrate
docker compose run --rm airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```
### 2. Start Airflow
```bash
docker compose up -d
```

### 3. Access the UI
Open:  
http://localhost:8080

Login with:
- **Username**: admin
- **Password**: admin
