with date_table as (
    {{ dbt_date.get_date_dimension("2024-12-01", "2026-12-31") }}
)

select
    date_day,
    day_of_week,
    day_of_week_name,
    day_of_week_name_short,
    case 
        when day_of_week_name_short in ('Sat', 'Sun') then True
        else False
    end as is_weekend,
    month_of_year,
    month_name,
    month_name_short,
    quarter_of_year,
    year_number,
    case 
        when month_of_year in (6, 7, 8) then 'Summer Peak'
        when month_of_year in (12, 1, 2) then 'Winter Peak'
        else 'Off-Peak'
    end as energy_season
from date_table