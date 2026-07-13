with raw_weather as (
    select 
        *,
        _PARTITIONTIME
    from {{ source('weather_source', 'weather_raw') }}
),

renamed as (
    select
        -- EIA subba identifiers
        subba as subba_id,
        
        -- Timestamps
        cast(timestamp_utc as timestamp) as timestamp_utc,
        
        -- Weather Metrics
        temp_f as temperature_fahrenheit,
        humidity_pct as relative_humidity_pct,
        apparent_temp_f as apparent_temperature_fahrenheit,
        wind_speed_mph,
        
        -- Metadata: Tracking coordinate drift
        request_lat as target_latitude,
        request_lon as target_longitude,
        actual_lat as actual_latitude,
        actual_lon as actual_longitude,

        CAST(_PARTITIONTIME AS DATE) as ingestion_date
        
    from raw_weather
)

select * from renamed
QUALIFY ROW_NUMBER() OVER(PARTITION BY timestamp_utc, subba_id ORDER BY ingestion_date DESC)=1
