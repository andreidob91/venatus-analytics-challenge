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
        
        -- Aggregate across all campaigns/devices/countries
        -- RENAME to avoid collision with source column names
        sum(total_events) as total_events_sum,
        sum(impressions) as impressions_sum,
        sum(filled_impressions) as filled_impressions_sum,
        sum(unfilled_impressions) as unfilled_impressions_sum,
        sum(clicks) as clicks_sum,
        sum(viewable_impressions) as viewable_impressions_sum,
        
        sum(total_revenue_usd) as revenue_sum,
        avg(avg_revenue_usd) as revenue_avg,
        
        sum(suspicious_events) as suspicious_events_sum,
        sum(negative_revenue_events) as negative_revenue_events_sum
        
    from {{ ref('fct_ad_events_daily') }}
    group by 
        event_date,
        publisher_id
)

select
    event_date,
    publisher_id,
    
    -- Rename back to clean names
    total_events_sum as total_events,
    impressions_sum as impressions,
    filled_impressions_sum as filled_impressions,
    unfilled_impressions_sum as unfilled_impressions,
    clicks_sum as clicks,
    viewable_impressions_sum as viewable_impressions,
    revenue_sum as total_revenue_usd,
    round(revenue_avg, 4) as avg_revenue_usd,
    suspicious_events_sum as suspicious_events,
    negative_revenue_events_sum as negative_revenue_events,
    
    -- Calculate rates (no collision now)
    if(impressions_sum > 0, round(filled_impressions_sum * 100.0 / impressions_sum, 2), 0) as fill_rate_pct,
    if(impressions_sum > 0, round(clicks_sum * 100.0 / impressions_sum, 4), 0) as ctr_pct,
    if(impressions_sum > 0, round(viewable_impressions_sum * 100.0 / impressions_sum, 2), 0) as viewability_rate_pct
    
from daily_base
