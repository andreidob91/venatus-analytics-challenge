{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

with campaigns as (
    select * from {{ ref('stg_campaigns') }}
),

campaign_metrics as (
    select
        campaign_id,
        count(*) as total_events,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(total_revenue_usd) as total_revenue
    from {{ ref('fct_ad_events_daily') }}
    where campaign_id != 0  -- Exclude unfilled
    group by campaign_id
),

final as (
    select
        c.campaign_id,
        c.campaign_name,
        c.advertiser_id,
        c.advertiser_name,
        c.campaign_start_date,
        c.campaign_end_date,
        c.campaign_budget_usd,
        c.campaign_status,
        c.targeting_device_types,
        c.targeting_countries,
        c.created_at,
        
        -- Metrics
        coalesce(m.total_events, 0) as total_events,
        coalesce(m.total_impressions, 0) as total_impressions,
        coalesce(m.total_clicks, 0) as total_clicks,
        coalesce(m.total_revenue, 0) as total_revenue,
        
        -- Calculated
        case 
            when m.total_impressions > 0 
            then round(m.total_clicks * 100.0 / m.total_impressions, 4)
            else 0 
        end as campaign_ctr_pct,
        
        case
            when c.campaign_budget_usd > 0
            then round((m.total_revenue / c.campaign_budget_usd) * 100.0, 2)
            else 0
        end as budget_utilization_pct
        
    from campaigns c
    left join campaign_metrics m on c.campaign_id = m.campaign_id
)

select * from final
