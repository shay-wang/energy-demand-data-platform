with base_snapshot as (
    select * from {{ ref('regions_snapshot') }}
),

backfilled as (
select
    subba_id,
    subba_name,
    balancing_authority_id,
    balancing_authority_name,
    timezone,
    -- If this is the oldest record for this subba, set it to 2024-01-01
    -- otherwise use the real snapshot time.
    case 
        when dbt_valid_from = (
            select min(dbt_valid_from) 
            from base_snapshot s2 
            where s2.subba_id = s.subba_id
        ) then '2024-01-01'
        else dbt_valid_from 
    end as valid_from,

    coalesce(dbt_valid_to, '9999-12-31') as valid_to,
    dbt_updated_at as last_modified_at,
    case when dbt_valid_to is null then true else false end as is_current,
from base_snapshot s
)

select
    {{ dbt_utils.generate_surrogate_key(['subba_id', 'valid_from']) }} as region_key,
    subba_id,
    subba_name,
    balancing_authority_id,
    balancing_authority_name,
    timezone,
    valid_from,
    valid_to,
    last_modified_at,
    is_current
from backfilled