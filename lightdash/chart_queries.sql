-- ============================================================================
-- Chart 1: Revenue Over Time - Top 5 Publishers
-- ============================================================================
-- Description: Daily revenue trends for top 5 publishers by lifetime revenue
WITH top_publishers AS (
    SELECT publisher_id
    FROM analytics_analytics.fct_publisher_performance
    GROUP BY publisher_id
    ORDER BY SUM(total_revenue_usd) DESC
    LIMIT 5
),

publisher_data AS (
    SELECT 
        f.event_date,
        f.total_revenue_usd,
        CASE 
            WHEN p.publisher_name = 'GameSpot Digital' THEN 'GameSpot'
            WHEN p.publisher_name = 'IGN Entertainment' THEN 'IGN'
            WHEN p.publisher_name = 'Polygon Media' THEN 'Polygon'
            WHEN p.publisher_name = 'Eurogamer Network' THEN 'Eurogamer'
            WHEN p.publisher_name = 'Kotaku Digital' THEN 'Kotaku'
            WHEN p.publisher_name = 'The Gamer Network' THEN 'The Gamer'
            ELSE p.publisher_name
        END AS publisher
    FROM analytics_analytics.fct_publisher_performance f
    JOIN analytics_analytics.dim_publishers p 
        ON f.publisher_id = p.publisher_id
    WHERE f.publisher_id IN (SELECT publisher_id FROM top_publishers)
)

SELECT
    event_date,
    publisher,
    SUM(total_revenue_usd) AS revenue
FROM publisher_data
GROUP BY event_date, publisher
ORDER BY event_date, publisher;


-- ============================================================================
-- Chart 2: Fill Rate Performance by Publisher
-- ============================================================================
-- Description: Shows fill rate percentage for each publisher to identify monetization efficiency
SELECT 
    p.publisher_name AS publisher,
    ROUND(
        SUM(f.filled_impressions) * 100.0 / NULLIF(SUM(f.impressions), 0),
        2
    ) AS fill_rate_pct
FROM analytics_analytics.fct_publisher_performance f
JOIN analytics_analytics.dim_publishers p
    ON f.publisher_id = p.publisher_id
GROUP BY p.publisher_name
ORDER BY fill_rate_pct DESC;

-- ============================================================================
-- Chart 3: Click Fraud Risk Assessment
-- ============================================================================
-- Description: Identifies publishers with suspicious CTR patterns
-- Normal CTR: 0.1-0.5% for display advertising
-- Threshold: CTR > 10% flagged as high risk
-- CTR calculated as: (clicks / impressions) * 100
-- Sorted by CTR descending to show worst offenders first

SELECT 
    p.publisher_name AS publisher,
    ROUND(p.clicks * 100.0 / NULLIF(p.impressions, 0), 2) AS ctr_pct,
    p.clicks,
    p.impressions,
    CASE 
        WHEN p.clicks * 100.0 / NULLIF(p.impressions, 0) > 100 THEN 'CRITICAL - Fraud'
        WHEN p.clicks * 100.0 / NULLIF(p.impressions, 0) > 10 THEN 'High Risk'
        WHEN p.clicks * 100.0 / NULLIF(p.impressions, 0) > 5 THEN 'Monitor'
        ELSE 'Normal'
    END AS risk_level
FROM analytics_analytics.dim_publishers p
WHERE p.impressions > 10
ORDER BY ctr_pct DESC
LIMIT 15;
