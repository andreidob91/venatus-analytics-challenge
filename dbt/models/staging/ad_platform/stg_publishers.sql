{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('ad_platform', 'publishers') }}
),

deduplicated as (
    -- Handle duplicate publisher_ids by keeping most recent update
    -- Note: Publisher 7 has historical name change (SCD Type 2 pattern)
    -- but we keep only current state for simplicity
    select 
        *,
        row_number() over (
            partition by publisher_id 
            order by updated_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        publisher_id,
        publisher_name,
        publisher_category,
        primary_domain,
        account_manager,
        country,
        created_at,
        updated_at
    from deduplicated
    where row_num = 1  -- Keep only the most recent record per publisher_id
)

select * from cleaned
