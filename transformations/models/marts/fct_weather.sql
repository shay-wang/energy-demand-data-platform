{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "timestamp_utc",
            "data_type": "timestamp",
            "granularity": "day"
        }
    )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_weather') }}
    {% if is_incremental() %}
    WHERE ingestion_date >= (SELECT MAX(last_updated_at) from {{ this }})
    {% endif %}
),

dim_region AS (
    SELECT * FROM {{ ref('dim_region') }}
)

SELECT
    dim.region_key,
    CAST(DATETIME(f.timestamp_utc, dim.timezone) AS DATE) AS date_key,
    EXTRACT(HOUR FROM DATETIME(f.timestamp_utc, dim.timezone)) AS hour_key,
    DATETIME(f.timestamp_utc, dim.timezone) AS datetime_local,
    f.timestamp_utc,

    -- Facts
    f.temperature_fahrenheit,
    f.relative_humidity_pct,
    f.apparent_temperature_fahrenheit,
    f.wind_speed_mph,

    -- Metadata
    f.ingestion_date AS last_updated_at

from staging f
left join dim_region dim
    on f.subba_id = dim.subba_id
    and f.timestamp_utc >= dim.valid_from 
    and f.timestamp_utc < dim.valid_to
