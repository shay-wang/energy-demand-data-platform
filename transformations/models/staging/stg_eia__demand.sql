WITH source_data AS (
    SELECT
        PARSE_TIMESTAMP('%Y-%m-%dT%H', period) as timestamp_utc,
        subba AS subba_id,
        `subba-name` AS subba_name,
        parent AS balancing_authority_id,
        `parent-name` AS balancing_authority_name,
        CAST(value AS FLOAT64) AS demand_mw,
    FROM {{ source('eia_source', 'eia_raw') }}
)

SELECT * FROM source_data
