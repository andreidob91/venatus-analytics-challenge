# Analytics Engineering Design Document
**Venatus Take-Home Challenge**

## Executive Summary

Built a production-ready dbt analytics layer transforming 131K+ ad-serving events from Venatus's programmatic platform. The project addresses critical data quality issues including duplicate events (1.52%), click fraud (8 publishers with 10-712% CTR), and negative revenue values. The final deliverable includes 4 staging models, 2 mart models, 28 comprehensive tests, and clean documentation.

**Key Findings:**
- 2,000 duplicate events requiring deduplication
- Publisher 20 shows impossible 712% CTR (100 clicks on 24 impressions) - clear fraud
- 151 events with negative revenue totaling -$214.19
- 13.9% unfilled impression rate (normal for programmatic advertising)

---

## Data Modeling Approach

### Architecture Overview
```
raw (ClickHouse)
  ├── ad_events (131,480 events)
  ├── publishers (20 publishers)
  ├── campaigns (27 campaigns)
  └── ad_units (60 ad units)
       ↓
staging layer (views)
  ├── stg_ad_events (deduplication + fraud flags)
  ├── stg_publishers (deduplication, SCD Type 2 handling)
  ├── stg_campaigns
  └── stg_ad_units
       ↓
marts layer (tables)
  ├── dim_publishers (dimension with lifetime metrics)
  └── fct_ad_events_daily (daily aggregated facts)
```

### Staging Layer Design

**Philosophy**: Light transformations, deduplication, type casting, and data quality flagging.

**Key Decisions:**

1. **Deduplication Strategy**
   - Used `ROW_NUMBER()` partitioned by primary key, ordered by `_loaded_at DESC`
   - Keeps most recent record in case of ETL re-runs
   - Applied to both `stg_ad_events` (2,000 dupes) and `stg_publishers` (Publisher 7 duplicate)

2. **SCD Type 2 Consideration**
   - Publisher 7 showed evidence of SCD Type 2 (name change: "PC Gamer Online" → "PC Gamer Digital")
   - **Decision**: Keep only current state for simplicity
   - **Rationale**: 2-4 hour challenge scope; production would implement full SCD Type 2 with valid_from/valid_to

3. **Data Quality Flags**
   - `is_suspicious_traffic`: Flags publishers with CTR > 10%
   - `has_negative_revenue`: Flags events with revenue < 0
   - Enables filtering without losing visibility into data issues

### Marts Layer Design

**Philosophy**: Business-logic enriched, query-optimized tables ready for BI consumption.

**Grain Decisions:**

- **`fct_ad_events_daily`**: Daily grain at publisher × campaign × device × country level
  - **Why daily?** Balances analytical flexibility with performance
  - **Why this grain?** Covers 90% of business questions while keeping table size manageable
  - **Alternative considered**: Event-level (too large, 131K+ rows), hourly (unnecessary granularity)

- **`dim_publishers`**: One row per publisher with lifetime aggregated metrics
  - Includes fraud risk scoring (`is_high_risk_publisher` for CTR > 10%)
  - Pre-calculated lifetime fill rate and CTR for quick analysis

**Calculated Metrics in Fact Table:**
- `fill_rate_pct`: (filled_impressions / total_impressions) × 100
- `ctr_pct`: (clicks / impressions) × 100  
- `viewability_rate_pct`: (viewable_impressions / impressions) × 100

**Rationale**: Centralize business logic in dbt for consistency across all downstream tools.

---

## Data Quality Issues & Solutions

### Issue 1: Duplicate Events

**Problem**:
- 2,000 duplicate event_ids (1.52% of data)
- Same event recorded multiple times with different `_loaded_at` timestamps

**Root Cause Analysis**:
- ETL pipeline retry logic or source system duplication
- Duplicates are exact copies (same timestamp, revenue) but different load times

**Business Impact**:
- Revenue double-counting: Could overstate revenue by 1.52%
- Inflated impression/click metrics
- Incorrect publisher payouts and advertiser billing

**Solution Implemented**:
```sql
deduplicated as (
    select 
        *,
        row_number() over (
            partition by event_id 
            order by _loaded_at desc
        ) as row_num
    from source
),
cleaned as (
    select * from deduplicated
    where row_num = 1
)
```

**Production Recommendations**:
1. Add UNIQUE constraint on `event_id` at database level
2. Investigate ETL pipeline for root cause
3. Implement idempotent upsert logic (INSERT ... ON DUPLICATE KEY UPDATE)
4. Add monitoring: alert if duplicate rate exceeds 1%

---

### Issue 2: Click Fraud / Invalid Traffic

**Problem**:
Publisher 20 (pocketgamer.com) shows **712% CTR** (100 clicks on 24 impressions) - physically impossible.

**Full List of Suspicious Publishers**:
| Publisher ID | Domain | CTR | Classification |
|-------------|--------|-----|----------------|
| 20 | pocketgamer.com | 712.5% | **IMPOSSIBLE** - Bot/fraud |
| 15 | attackofthefanboy.com | 40.9% | Highly suspicious |
| 11 | thegamer.com | 33.9% | Highly suspicious |
| 9 | vg247.com | 33.9% | Highly suspicious |
| 19 | toucharcade.com | 33.8% | Highly suspicious |
| 17 | pushsquare.com | 31.4% | Highly suspicious |
| 5 | polygon.com | 21.2% | Suspicious |
| 13 | gamerant.com | 19.0% | Suspicious |

**Industry Context**: Normal CTR is 0.1-0.5%; anything above 2-3% warrants investigation.

**Business Impact**:
- Advertisers paying for fraudulent clicks
- Estimated impact: ~$10K+ in wasted ad spend (40% of total click revenue from suspicious publishers)
- Platform reputation risk
- Potential contract violations

**Solution Implemented**:
1. Added `is_suspicious_traffic` flag in staging
2. Created custom test (`suspicious_ctr_check.sql`) that **intentionally fails** to alert on fraud
3. Calculated `revenue_usd_valid_traffic_only` metric excluding suspicious publishers

**Production Recommendations**:
1. **Immediate**: Quarantine Publisher 20, investigate Publishers 15, 11, 9
2. Implement real-time fraud detection (alert on CTR > 5%)
3. Add IP address analysis and bot detection
4. Consider third-party fraud detection (IAS, DoubleVerify, White Ops)
5. Review and potentially terminate contracts with high-CTR publishers
6. Add CAPTCHA or challenge-response to publisher sites

---

### Issue 3: Negative Revenue

**Problem**:
- 151 events with negative revenue
- Total negative revenue: -$214.19 (0.11% of events)
- Range: -$2.49 to -$2.16

**Affected Publishers**: ign.com, eurogamer.net, polygon.com, pcgamer.com, gamespot.com

**Potential Root Causes**:
1. Refunds/chargebacks incorrectly recorded in events table
2. System bug in revenue calculation
3. Currency conversion errors
4. Data pipeline transformation issue

**Business Impact**:
- Small financial impact (-$214) but indicates data integrity issues
- Breaks reconciliation with financial systems
- Potential downstream reporting errors

**Solution Implemented**:
- Added `has_negative_revenue` flag for visibility
- Kept negative values (didn't filter out) to maintain data completeness
- Added test to monitor negative revenue count

**Production Recommendations**:
1. Add CHECK constraint: `revenue_usd >= 0` at database level
2. Investigate with engineering team to find root cause
3. If refunds/adjustments, create separate `refunds` table
4. Add validation in ETL pipeline before loading

---

### Pattern 4: Unfilled Impressions (NOT AN ISSUE)

**Observation**:
- 18,278 events (13.9%) with NULL `campaign_id` and `advertiser_id`
- Perfect correlation with `is_filled = 0`

**Analysis**: This is **normal and expected**:
- When no advertiser wins auction, ad slot remains unfilled
- NULL values are correct business logic
- 86.1% fill rate is healthy for programmatic advertising

**Handling**: Use LEFT JOINs and COALESCE when joining dimensions; track fill rate as KPI.

---

## Technical Implementation Details

### ClickHouse-Specific Challenges

**Challenge 1: Nested Aggregate Functions**

ClickHouse does not allow aggregate functions inside other aggregate functions in the same query.

**Problem Code** (fails):
```sql
sum(if(revenue_usd > 0, revenue_usd, 0)) as positive_revenue  -- ERROR!
```

**Solution**: Use CTE pattern to separate aggregation from calculation:
```sql
with aggregated as (
    select
        sum(if(event_type = 'impression', 1, 0)) as impressions,
        sum(if(event_type = 'click', 1, 0)) as clicks
    from events
    group by date
)
select
    *,
    if(impressions > 0, clicks * 100.0 / impressions, 0) as ctr
from aggregated
```

**Challenge 2: Column Name Collision**

Naming an aggregated column the same as its source column causes ClickHouse to detect "nested aggregates."

**Problem Code** (fails):
```sql
sum(revenue_usd) as revenue_usd  -- Collision!
```

**Solution**: Rename aggregated columns:
```sql
sum(revenue_usd) as total_revenue_usd  -- Works!
```

---

## Testing Strategy

### Test Coverage

**Total Tests**: 28
- **Passing**: 27
- **Expected Failures**: 1 (`suspicious_ctr_check` - fraud detection)

**Test Types Implemented**:

1. **Generic Tests** (from `schema.yml`):
   - `unique`: Primary keys in all staging and dimension tables
   - `not_null`: Required fields
   - `accepted_values`: Categorical fields (event_type, is_filled, etc.)
   - `relationships`: Foreign key integrity (publisher_id, campaign_id)

2. **Singular Tests** (custom SQL):
   - `suspicious_ctr_check.sql`: Detects publishers with CTR > 100%
   - **This test FAILS by design** - it's an alert, not a data blocker

### Test Results Summary
```
✅ All staging models have unique primary keys
✅ All required fields are not null
✅ Event types are valid (impression, click, viewable_impression)
✅ Foreign key relationships validated
❌ Fraud detected: Publisher 20 has 712% CTR (EXPECTED FAILURE)
```

---

## Trade-Offs & Shortcuts

### What I Didn't Build (Time Constraints)

1. **Additional Dimensions**
   - `dim_campaigns` - would enrich with budget utilization, campaign performance
   - `dim_ad_units` - would add placement analysis
   - `dim_dates` - would enable time-series analysis patterns

2. **Additional Fact Tables**
   - `fct_ad_events_hourly` - for intraday pacing analysis
   - `fct_publisher_performance_weekly` - for publisher business reviews
   - `fct_campaign_performance` - for advertiser reporting

3. **Advanced Features**
   - Incremental models (used table materialization due to ClickHouse challenges)
   - Snapshots for SCD Type 2 tracking
   - dbt-utils surrogate keys
   - Data observability (freshness checks, anomaly detection)

### What I'd Change With More Time

1. **Implement Full SCD Type 2**
   - Track historical changes to publisher names/attributes
   - Add `valid_from`, `valid_to`, `is_current` columns
   - Use dbt snapshots

2. **Enhanced Data Quality**
   - Expand test coverage to 100% of columns
   - Add dbt-expectations package for statistical tests
   - Implement data profiling reports
   - Add freshness checks on source tables

3. **Performance Optimization**
   - Switch to incremental models with proper backfill strategy
   - Add ClickHouse-specific optimizations (partitioning, clustering)
   - Profile query performance and add indexes
   - Implement cost monitoring

4. **Business Logic Enhancements**
   - More granular fraud scoring (not just binary flag)
   - Cohort analysis for publisher performance trends
   - Attribution modeling for multi-touch campaigns
   - Revenue reconciliation tables

---

## Production Readiness Roadmap

### To Deploy This to Production

**Phase 1: Infrastructure (Week 1)**
- [ ] Set up CI/CD pipeline (GitHub Actions or dbt Cloud)
- [ ] Configure dev/staging/prod environments
- [ ] Implement secrets management (dbt Cloud or environment variables)
- [ ] Set up monitoring and alerting (dbt artifacts + Monte Carlo/Datafold)
- [ ] Configure incremental models with proper backfill strategy

**Phase 2: Data Quality (Week 2)**
- [ ] Expand test coverage to 100% of critical columns
- [ ] Add freshness checks on all source tables
- [ ] Implement anomaly detection on key metrics (revenue, CTR, fill rate)
- [ ] Set up data quality dashboards
- [ ] Define SLAs and document in schema.yml

**Phase 3: Performance & Scale (Week 3)**
- [ ] Profile query performance and optimize slow models
- [ ] Add ClickHouse partitioning on event_date
- [ ] Implement clustering keys for common query patterns
- [ ] Set up cost monitoring and FinOps governance
- [ ] Optimize incremental model run times

**Phase 4: Documentation & Governance (Week 4)**
- [ ] Generate and host dbt docs site
- [ ] Create data dictionary for business users
- [ ] Document runbooks for common issues
- [ ] Define data ownership and escalation paths
- [ ] Implement row-level security and access controls
- [ ] Create incident response process

**Phase 5: Advanced Features (Ongoing)**
- [ ] Build additional dimensions and facts
- [ ] Implement ML-ready feature tables
- [ ] Add real-time streaming for critical metrics
- [ ] Build data quality scorecards
- [ ] Integrate with reverse ETL for operational use cases

---

## Dashboard Insights

### Revenue Overview
- Total revenue across 37 days: ~$35,098
- Revenue model: Primarily CPC-based ($2.48 avg per click)
- Top revenue-generating publishers: gamespot.com, ign.com, polygon.com

### Fill Rate Analysis
- Overall fill rate: 86.1% (healthy for programmatic)
- Best fill rate: [To be determined from Lightdash analysis]
- Lowest fill rate: [To be determined from Lightdash analysis]

### Fraud & Data Quality
- 8 publishers flagged for suspicious traffic
- Estimated fraudulent revenue: ~$10K+ (needs deeper investigation)
- Data quality score: 98.4% (after deduplication and filtering)

---

## Conclusion

This project demonstrates a production-ready approach to analytics engineering:
- **Comprehensive data quality handling** with clear documentation of issues and solutions
- **Thoughtful dimensional modeling** balancing simplicity with analytical power
- **Robust testing strategy** catching data issues before they reach business users
- **Clear documentation** enabling knowledge transfer and maintainability

The most critical finding is the click fraud issue, which represents both a financial and reputational risk to Venatus. Immediate action on Publisher 20 and investigation of the other 7 flagged publishers is recommended.

**Next Steps**: Deploy to production with the roadmap outlined above, starting with fraud mitigation and CI/CD setup.

---

## Appendix

### Technologies Used
- **dbt-clickhouse** (1.11.7): Transformation framework
- **ClickHouse** (1.10.0): Columnar OLAP database
- **Lightdash**: dbt-native BI tool
- **Docker**: Local development environment
- **Git**: Version control

### Data Lineage
```
raw.ad_events (131,480 events)
    ↓ [deduplication, fraud flagging]
stg_ad_events (129,480 clean events)
    ↓ [daily aggregation]
fct_ad_events_daily (~3,700 rows)
    ↓ [BI consumption]
Lightdash Dashboard

raw.publishers (20 publishers, 1 duplicate)
    ↓ [deduplication]
stg_publishers (20 clean publishers)
    ↓ [lifetime metrics enrichment]
dim_publishers (20 rows with metrics)
```

### Key SQL Patterns Used
- Window functions for deduplication (`ROW_NUMBER() OVER`)
- Conditional aggregation (`SUM(IF(...))` in ClickHouse)
- CTE pattern to avoid nested aggregates
- COALESCE for NULL handling
- LEFT JOINs for dimension enrichment

### Repository Structure
```
venatus-analytics-challenge/
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   └── ad_platform/
│   │   │       ├── sources.yml
│   │   │       ├── schema.yml
│   │   │       ├── stg_ad_events.sql
│   │   │       ├── stg_publishers.sql
│   │   │       ├── stg_campaigns.sql
│   │   │       └── stg_ad_units.sql
│   │   └── marts/
│   │       └── core/
│   │           ├── schema.yml
│   │           ├── dim_publishers.sql
│   │           └── fct_ad_events_daily.sql
│   └── tests/
│       └── suspicious_ctr_check.sql
├── EXPLORATION_NOTES.md
├── DESIGN.md
└── README.md
```
