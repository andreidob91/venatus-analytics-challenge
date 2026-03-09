{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('ad_platform', 'campaigns') }}
),

cleaned as (
    select
        campaign_id,
        campaign_name,
        advertiser_id,
        advertiser_name,
        campaign_start_date,
        campaign_end_date,
        campaign_budget_usd,
        campaign_status,
        targeting_device_types,
        targeting_countries,
        created_at
    from source
)

select * from cleaned
