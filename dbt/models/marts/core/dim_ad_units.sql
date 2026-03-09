{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

with ad_units as (
    select * from {{ ref('stg_ad_units') }}
),

final as (
    select
        ad_unit_id,
        publisher_id,
        ad_unit_name,
        ad_format,
        ad_size,
        placement_type,
        is_active,
        created_at
    from ad_units
)

select * from final
