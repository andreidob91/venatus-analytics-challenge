{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

with publishers as (
    select * from {{ ref('stg_publishers') }}
),

publisher_metrics as (
    select
        publisher_id,
        count(*) as total_events,
        sum(case when is_filled = 1 then 1 else 0 end) as filled_events,
        sum(case when event_type = 'impression' then 1 else 0 end) as impressions,
        sum(case when event_type = 'click' then 1 else 0 end) as clicks,
        sum(revenue_usd) as lifetime_revenue,
        sum(case when is_suspicious_traffic = 1 then 1 else 0 end) as suspicious_events,
        sum(case when has_negative_revenue = 1 then 1 else 0 end) as negative_revenue_events
    from {{ ref('stg_ad_events') }}
    group by publisher_id
),

final as (
    select
        p.publisher_id,
        p.publisher_name,
        p.publisher_category,
        p.primary_domain,
        p.account_manager,
        p.country,
        p.created_at,
        p.updated_at,
        
        -- Aggregated metrics
        coalesce(m.total_events, 0) as total_events,
        coalesce(m.filled_events, 0) as filled_events,
        coalesce(m.impressions, 0) as impressions,
        coalesce(m.clicks, 0) as clicks,
        coalesce(m.lifetime_revenue, 0) as lifetime_revenue,
        coalesce(m.suspicious_events, 0) as suspicious_events,
        coalesce(m.negative_revenue_events, 0) as negative_revenue_events,
        
        -- Calculated metrics
        case 
            when m.total_events > 0 
            then round(m.filled_events * 100.0 / m.total_events, 2)
            else 0 
        end as lifetime_fill_rate_pct,
        
        case 
            when m.impressions > 0 
            then round(m.clicks * 100.0 / m.impressions, 4)
            else 0 
        end as lifetime_ctr_pct,
        
        case 
            when m.total_events > 0
            then round(m.suspicious_events * 100.0 / m.total_events, 2)
            else 0
        end as suspicious_traffic_pct,
        
        -- Data quality flags
        case 
            when m.impressions > 0 and (m.clicks * 100.0 / m.impressions) > 10
            then 1
            else 0
        end as is_high_risk_publisher
        
    from publishers p
    left join publisher_metrics m 
        on p.publisher_id = m.publisher_id
)

select * from final
