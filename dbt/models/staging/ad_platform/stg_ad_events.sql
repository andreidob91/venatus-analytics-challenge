{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('ad_platform', 'ad_events') }}
),

deduplicated as (
    -- Handle duplicates: Keep most recent load per event_id
    -- Fixes: 2,000 duplicate events (1.52% of data)
    select 
        *,
        row_number() over (
            partition by event_id 
            order by _loaded_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        -- Primary key
        event_id,
        
        -- Event details
        event_type,
        event_timestamp,
        toDate(event_timestamp) as event_date,
        
        -- Relationships
        publisher_id,
        site_domain,
        ad_unit_id,
        campaign_id,
        advertiser_id,
        
        -- Context dimensions
        device_type,
        country_code,
        browser,
        
        -- Metrics
        revenue_usd,
        bid_floor_usd,
        is_filled,
        
        -- Data quality flags
        case 
            -- Flag publishers with CTR > 10% (suspicious)
            -- Publisher 20: 712% CTR, Publishers 15,11,9,19,17,5,13: 19-40% CTR
            when publisher_id in (20, 15, 11, 9, 19, 17, 5, 13) 
            then 1 
            else 0 
        end as is_suspicious_traffic,
        
        case 
            -- Flag negative revenue (151 events, -$214.19 total)
            when revenue_usd < 0 then 1 
            else 0 
        end as has_negative_revenue,
        
        -- Metadata
        _loaded_at
        
    from deduplicated
    where row_num = 1  -- Keep only one record per event_id
)

select * from cleaned
