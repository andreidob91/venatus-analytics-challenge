{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('ad_platform', 'ad_units') }}
),

cleaned as (
    select
        ad_unit_id,
        publisher_id,
        ad_unit_name,
        ad_format,
        ad_size,
        placement_type,
        is_active,
        created_at
    from source
)

select * from cleaned
