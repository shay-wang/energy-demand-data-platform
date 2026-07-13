with base as (
    select * from {{ ref('ref_hours') }}
)

select
    hour_key,
    hour_ampm,
    -- Define Peak vs Off-Peak
    case 
        when hour_key between 6 and 9 then true
        when hour_key between 16 and 20 then true
        else false
    end as is_peak_hour,
    -- Define specific time-of-day categories
    case
        when hour_key between 0 and 5 then 'Night'
        when hour_key between 6 and 11 then 'Morning'
        when hour_key between 12 and 16 then 'Afternoon'
        when hour_key between 17 and 21 then 'Evening'
        else 'Night'
    end as time_of_day_name,
    -- Energy-specific peak naming
    case
        when hour_key between 16 and 20 then 'Evening Peak'
        when hour_key between 6 and 9 then 'Morning Peak'
        else 'Off-Peak'
    end as peak_type
from base