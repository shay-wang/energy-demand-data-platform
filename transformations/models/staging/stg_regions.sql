with base_seed as (
    select * from {{ ref('ref_regions') }}
),

metadata as (
    -- Get the most recent metadata from the raw table
    -- Using QUALIFY to get exactly one row per subba
    select 
        subba AS subba_id,
        `subba-name` AS subba_name,
        parent AS balancing_authority_id,
        `parent-name` AS balancing_authority_name,
    from {{ source('eia_source', 'eia_raw') }}
    qualify row_number() over (partition by subba order by _PARTITIONTIME desc) = 1
)

select 
    b.subba_id,
    m.subba_name,
    m.balancing_authority_id,
    m.balancing_authority_name,
    b.timezone
from base_seed b
left join metadata m on b.subba_id = m.subba_id