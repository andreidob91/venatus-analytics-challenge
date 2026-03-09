{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('ad_platform', 'publishers') }}
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
    from source
)

select * from cleaned
