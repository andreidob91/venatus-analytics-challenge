-- Test that no publisher has CTR > 100% 
-- (impossible: can't have more clicks than impressions)
-- This test SHOULD FAIL and catch Publisher 20 with 712% CTR

with publisher_metrics as (
    select
        publisher_id,
        sum(case when event_type = 'impression' then 1 else 0 end) as impressions,
        sum(case when event_type = 'click' then 1 else 0 end) as clicks,
        case 
            when sum(case when event_type = 'impression' then 1 else 0 end) > 0
            then sum(case when event_type = 'click' then 1 else 0 end) * 100.0 / 
                 sum(case when event_type = 'impression' then 1 else 0 end)
            else 0
        end as ctr_pct
    from {{ ref('stg_ad_events') }}
    group by publisher_id
)

select 
    publisher_id,
    impressions,
    clicks,
    round(ctr_pct, 2) as ctr_pct
from publisher_metrics
where ctr_pct > 100  -- This should catch Publisher 20 with 712% CTR
order by ctr_pct desc
