{{
    config(
        materialized='table',
        tags=['marts', 'facts']
    )
}}

with daily_base as (
    select
        event_date,
        publisher_id,
        site_domain,
        coalesce(campaign_id, 0) as campaign_id,
        coalesce(advertiser_id, 0) as advertiser_id,
        device_type,
        country_code,
        
        count(*) as total_events,
        
        -- Impressions
        sum(if(event_type = 'impression', 1, 0)) as impressions,
        sum(if(event_type = 'impression' AND is_filled = 1, 1, 0)) as filled_impressions,
        sum(if(event_type = 'impression' AND is_filled = 0, 1, 0)) as unfilled_impressions,
        
        -- Clicks
        sum(if(event_type = 'click', 1, 0)) as clicks,
        
        -- Viewability
        sum(if(event_type = 'viewable_impression', 1, 0)) as viewable_impressions,
        
        -- Revenue
        sum(revenue_usd) as total_revenue_usd,
        round(avg(revenue_usd), 4) as avg_revenue_usd,
        sum(bid_floor_usd) as total_bid_floor_usd,
        
        -- Data quality flags
        sum(if(is_suspicious_traffic = 1, 1, 0)) as suspicious_events,
        sum(if(has_negative_revenue = 1, 1, 0)) as negative_revenue_events
        
    from {{ ref('stg_ad_events') }}
    group by 
        event_date,
        publisher_id,
        site_domain,
        campaign_id,
        advertiser_id,
        device_type,
        country_code
)

select
    event_date,
    publisher_id,
    site_domain,
    campaign_id,
    advertiser_id,
    device_type,
    country_code,
    
    total_events,
    impressions,
    filled_impressions,
    unfilled_impressions,
    clicks,
    viewable_impressions,
    
    total_revenue_usd,
    avg_revenue_usd,
    total_bid_floor_usd,
    
    suspicious_events,
    negative_revenue_events,
    
    -- Calculated percentages (now referencing the CTE columns, not nested aggregates)
    if(impressions > 0, round(filled_impressions * 100.0 / impressions, 2), 0) as fill_rate_pct,
    if(impressions > 0, round(clicks * 100.0 / impressions, 4), 0) as ctr_pct,
    if(impressions > 0, round(viewable_impressions * 100.0 / impressions, 2), 0) as viewability_rate_pct
    
from daily_base
