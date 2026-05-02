{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "timestamp_utc",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['balancing_authority_id', 'subba_id']
    )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_eia__demand') }}
    {% if is_incremental() %}
    WHERE ingestion_date >= (SELECT MAX(last_updated_at) from {{ this }})
    {% endif %}
),

dim_region AS (
    SELECT * FROM {{ ref('dim_region') }}
)

SELECT
    dim.region_key,
    CAST(f.timestamp_utc AS DATE) AS date_key,
    EXTRACT(HOUR FROM f.timestamp_utc) AS hour_key,
    f.subba_id,
    f.balancing_authority_id,
    f.timestamp_utc,
    
    -- Facts
    f.demand_mw,
    
    -- Metadata
    f.ingestion_date AS last_updated_at

FROM staging f
LEFT JOIN dim_region dim
    ON f.subba_id = dim.subba_id
    AND f.timestamp_utc >= dim.valid_from 
    AND f.timestamp_utc < dim.valid_to

