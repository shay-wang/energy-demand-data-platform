WITH source AS (
    SELECT 
        PARSE_TIMESTAMP('%Y-%m-%dT%H', `period`) AS timestamp_utc,
        subba AS subba_id,
        `subba-name` AS subba_name,
        parent AS balancing_authority_id,
        `parent-name` AS balancing_authority_name,
        CAST(value AS FLOAT64) AS demand_mw,
        CAST(_PARTITIONTIME AS DATE) as ingestion_date
    FROM {{ source('eia_source', 'eia_raw') }}
)

SELECT 
* 
FROM source
QUALIFY ROW_NUMBER() OVER(PARTITION BY timestamp_utc, subba_id ORDER BY ingestion_date DESC)=1
