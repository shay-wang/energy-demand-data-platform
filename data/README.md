# GCS Landing Zone Mock Directory

This directory contains a representative sample of the raw data layer extracted by Apache Airflow from the EIA and Open-Meteo APIs. It replicates the exact directory structure and file formatting used within the Google Cloud Storage (GCS) landing buckets.

## Storage Architecture & Partitioning

The storage layer enforces a **Hive-style partitioning scheme** (`year=YYYY/month=MM/day=DD/`) to optimize downstream BigQuery external table definitions, automated partition discovery, and cost-efficient incremental loading.

```bash
data/
├── eia/year=2026/month=02/day=10/
│   └── hourly_demand_NCEN.json     # Raw hourly grid metrics (NDJSON)
└── weather/year=2026/month=02/day=10/
    └── weather_NCEN.ndjson         # Matched coordinate metrics (NDJSON)