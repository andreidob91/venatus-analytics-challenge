# Data Exploration Notes
Analytics Engineering Take-Home Challenge - Venatus

**Date**: March 8, 2026
**Data Period**: February 6, 2026 - March 15, 2026 (37 days)
**Total Events**: 131,480 events
## Summary

Found 4 major data quality issues during exploration:
1. **Duplicate Events**: 2,000 duplicates (1.52% of data)
2. **Click Fraud/Bot Traffic**: Publisher with 712% CTR and 7 others with 10-40% CTR
3. **Negative Revenue**: 151 events with negative revenue totaling -$214.19
4. **Unfilled Impressions**: 13.9% unfilled (possibly normal)

## Data Quality Issue #1: Duplicate Events

### Query Used
```sql
-- Check for duplicates
SELECT 
    event_id, 
    count(*) as cnt 
FROM raw.ad_events 
GROUP BY event_id 
HAVING cnt > 1
LIMIT 10;
```

### Results
- Found duplicate event_ids with count = 2
- At least 10 event_ids shown in initial query

### Scale of Problem
```sql
-- Total duplicate count
SELECT 
    count(*) as total_events,
    count(DISTINCT event_id) as unique_events,
    count(*) - count(DISTINCT event_id) as duplicate_count,
    round((count(*) - count(DISTINCT event_id)) * 100.0 / count(*), 2) as duplicate_pct
FROM raw.ad_events;
```

**Results**:
- Total events: 131,480
- Unique events: 129,480
- Duplicates: 2,000 events
- **Duplicate percentage: 1.52%**
### Nature of Duplicates
```sql
-- Are duplicates identical or different?
SELECT 
    event_id,
    count(*) as cnt,
    count(DISTINCT event_timestamp) as distinct_timestamps,
    count(DISTINCT revenue_usd) as distinct_revenues,
    count(DISTINCT _loaded_at) as distinct_load_times
FROM raw.ad_events 
GROUP BY event_id 
HAVING cnt > 1
LIMIT 10;
```

**Findings**:
- cnt = 2 (each duplicate appears exactly twice)
- distinct_timestamps = 1 (same event timestamp - exact duplicates)
- distinct_revenues = 1 (same revenue value)
- distinct_load_times = 2 (different ETL load times)

**Conclusion**: Exact duplicate events loaded at different times by ETL pipeline.

### Business Impact
- Revenue double-counting
- Inflated impression/click metrics
- Incorrect reporting to clients and publishers
- Potential billing issues

### Solution Implemented
- Will Deduplicate in `stg_ad_events` using ROW_NUMBER() partitioned by event_id
- Keep most recent record based on `_loaded_at` timestamp
- Add unique test on event_id in staging layer

### Production Recommendations
1. Add UNIQUE constraint on event_id at database level
2. Investigate ETL pipeline for root cause of duplicates
3. Add monitoring alert if duplicate rate exceeds threshold (e.g., 1%)
4. Implement idempotent upsert logic instead of insert-only

---

## Data Quality Issue #2: Click Fraud / Bot Traffic

### Query Used
```sql
-- Check for suspicious CTR patterns
SELECT 
    publisher_id,
    site_domain,
    count(*) as events,
    sum(revenue_usd) as revenue,
    round(avg(revenue_usd), 4) as avg_revenue,
    round(sum(CASE WHEN is_filled = 1 THEN 1 ELSE 0 END) * 100.0 / count(*), 2) as fill_rate_pct,
    sum(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
    sum(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) as impressions,
    CASE 
        WHEN sum(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) > 0 
        THEN round(sum(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 / sum(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 4)
        ELSE 0 
    END as ctr_pct
FROM raw.ad_events
GROUP BY publisher_id, site_domain
ORDER BY ctr_pct DESC
LIMIT 20;
```

### Results - Suspicious Publishers

**CRITICAL - Impossible CTR**:
- **Publisher 20 (pocketgamer.com)**: 712.5% CTR
  - 100 clicks on only 24 impressions
  - **Physically impossible** - you cannot have more clicks than impressions!

**HIGHLY SUSPICIOUS - Abnormal CTR**:
- Publisher 15 (attackofthefanboy.com): 40.9% CTR
- Publisher 11 (thegamer.com): 33.9% CTR
- Publisher 9 (vg247.com): 33.9% CTR
- Publisher 19 (toucharcade.com): 33.8% CTR
- Publisher 17 (pushsquare.com): 31.4% CTR
- Publisher 5 (polygon.com): 21.2% CTR
- Publisher 13 (gamerant.com): 19.0% CTR

**Industry Context**:
- Normal CTR: 0.1% - 0.5%
- Suspicious threshold: Anything above 2-3%
- These publishers show 19% to 712% CTR

### Root Causes (Likely)
1. Bot traffic or click fraud
2. Misconfigured tracking (counting same click multiple times)
3. Iinjection of  fake clicks
4. Click farms or automated scripts

### Business Impact
- **Advertisers paying for fraudulent clicks**
- Skewed performance metrics and reporting
- Potential legal/contract violations
- Damage to platform reputation and trust
- Wasted advertiser budgets
- Risk of losing legitimate advertisers

### Solution Implemented
- Add `is_suspicious_traffic` flag in `stg_ad_events` for publishers with CTR > 10%
- Flag publisher IDs: 20, 15, 11, 9, 19, 17, 5, 13
- Create separate "valid traffic" vs "total traffic" metrics
- Add test to monitor CTR by publisher

### Production Recommendations
1. **Immediate**: Quarantine publishers with CTR > 5% for investigation
2. Implement real-time fraud detection algorithm
3. Add automated monitoring and alerts for CTR anomalies
4. Review and potentially terminate contracts with high-CTR publishers
5. Consider third-party fraud detection (IAS, DoubleVerify, White Ops)
6. Implement CAPTCHA or bot detection on publisher sites
7. Add IP address analysis for click patterns
8. Create fraud score for each publisher

---

## Data Quality Issue #3: Negative Revenue

### Query Used
```sql
-- Check for negative revenue
SELECT 
    is_filled,
    event_type,
    count(*) as events,
    sum(revenue_usd) as total_revenue,
    round(avg(revenue_usd), 6) as avg_revenue,
    min(revenue_usd) as min_revenue,
    max(revenue_usd) as max_revenue
FROM raw.ad_events
GROUP BY is_filled, event_type
ORDER BY is_filled DESC, event_type;
```

### Results
- Filled impressions show **min_revenue = -$2.49**
- Negative revenue only appears in filled impressions (is_filled = 1)
- Unfilled events correctly have $0 revenue

### Detailed Investigation
```sql
-- View specific negative revenue events
SELECT 
    event_id,
    event_type,
    publisher_id,
    site_domain,
    campaign_id,
    revenue_usd,
    bid_floor_usd,
    is_filled,
    event_timestamp
FROM raw.ad_events
WHERE revenue_usd < 0
ORDER BY revenue_usd ASC
LIMIT 20;
```

### Sample Negative Revenue Events
- Event types: All are impressions
- Revenue range: -$2.49 to -$2.16
- Publishers affected: ign.com, eurogamer.net, polygon.com, pcgamer.com, gamespot.com
- All have low bid_floor_usd (0.002)
- Distributed across February 2026

### Scale of Problem
```sql
-- Total negative revenue impact
SELECT 
    count(*) as negative_revenue_events,
    sum(revenue_usd) as total_negative_revenue,
    count(*) * 100.0 / (SELECT count(*) FROM raw.ad_events) as pct_of_total
FROM raw.ad_events
WHERE revenue_usd < 0;
```

**Results**:
- **151 events** with negative revenue
- **Total negative revenue: -$214.19**
- **Percentage: 0.11%** of all events

### Potential Causes
1. Refunds/chargebacks recorded in events table (should be separate)
2. System bug in revenue calculation logic
3. Data pipeline transformation error
4. Currency conversion issues
5. Incorrect data entry

### Business Impact
- Understates actual revenue (though small impact: -$214)
- Incorrect publisher payouts
- Financial reporting inaccuracies
- Breaks revenue reconciliation

### Solution Implemented
- Add `has_negative_revenue` flag in `stg_ad_events`
- Keep negative values (filter only extreme outliers < -$10)
- Add test to monitor negative revenue count
- Document for investigation with source system team

### Production Recommendations
1. Add CHECK constraint at database level: `revenue_usd >= 0`
2. Investigate with engineering team to find root cause
3. Create separate adjustments/refunds table if that's the cause
4. Add validation in ETL pipeline before loading
5. Set up alerts for any negative revenue events

---

## Pattern #4: Unfilled Impressions (NORMAL)

### Query Used
```sql
-- Check NULL patterns
SELECT 
    countIf(campaign_id IS NULL) as null_campaigns,
    countIf(advertiser_id IS NULL) as null_advertisers,
    countIf(is_filled = 0) as unfilled_events,
    count(*) as total
FROM raw.ad_events;
```

### Results
- null_campaigns: 18,278 (13.9%)
- null_advertisers: 18,278 (13.9%)
- unfilled_events: 18,278 (13.9%)
- **Perfect correlation**: NULL values = unfilled events

### Interpretation
This is **NORMAL and EXPECTED** behavior:
- When `is_filled = 0`, no advertiser won the auction
- Ad slot shows house ad or remains empty
- NULL campaign_id and advertiser_id are correct
- 86.1% fill rate is good?

### No Action Required
- Handle NULLs appropriately in joins (use LEFT JOIN, COALESCE)
- Track fill rate as KPI
- Not a data quality issue

---

## Revenue Model Analysis

### Query Used
```sql
-- Revenue by event type
SELECT 
    event_type,
    count(*) as events,
    sum(revenue_usd) as total_revenue,
    avg(revenue_usd) as avg_revenue
FROM raw.ad_events
GROUP BY event_type;
```

### Results

**Clicks** (CPC Model):
- 13,713 clicks
- Total revenue: $34,057.86
- **Average: $2.48 per click**

**Impressions** (CPM Model):
- 91,979 impressions
- Total revenue: $425.48
- **Average: $0.0046 per impression** (~$4.60 CPM)

**Viewable Impressions**:
- 25,788 viewable impressions
- Total revenue: $615.10
- **Average: $0.024 per viewable impression**

### Business Insight
- Platform is primarily **CPC-based** (Cost Per Click)
- Clicks generate 80x more revenue per event than impressions
- This explains why click fraud is such a critical issue

---

## Data Coverage

### Time Period
```sql
SELECT 
    min(event_timestamp) as earliest,
    max(event_timestamp) as latest,
    dateDiff('day', min(event_timestamp), max(event_timestamp)) as days_span
FROM raw.ad_events;
```

**Results**:
- Earliest: 2026-02-06 00:00:03
- Latest: 2026-03-15 23:42:34
- **Span: 37 days**

### Volume
- Total events: 131,480
- Average events per day: ~3,553

---

## Top Publishers by Volume

Top 5 publishers by event count:
1. gamespot.com: 14,755 events
2. ign.com: 14,714 events
3. eurogamer.net: 10,980 events
4. pcgamer.com: 10,991 events
5. polygon.com: 11,029 events

---

## Summary for DESIGN.md

### Critical Findings
1. ✅ **1.52% duplicate rate** - Fixed via deduplication
2. ✅ **Publisher 20 has 712% CTR**  - Flagged as suspicious
3. ✅ **151 negative revenue events** - Flagged for investigation
4. ✅ **13.9% unfilled rate** - Normal business pattern

### Data Quality Score
- **Clean data**: ~97% after deduplication
- **Valid traffic**: ~85% (excluding suspicious publishers)
- **Revenue integrity**: 99.9% (negative revenue minimal)

### Next Steps
All findings incorporated into dbt staging models with:
- Deduplication logic
- Fraud detection flags
- Data quality tests
- Comprehensive documentation

---

## Queries Reference

All queries used in this investigation are documented above in their respective sections.

