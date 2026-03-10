# Venatus Analytics Challenge - Solution
**Completed by**: Andrei-Costin Dobrin  
**Repository**: https://github.com/andreidob91/venatus-analytics-challenge  
**Date**: March 10, 2026

## 🎯 Solution Summary

Built a production-ready dbt analytics layer on ClickHouse with comprehensive data quality handling, fraud detection, and executive dashboards.

### 📊 Key Deliverables

1. **dbt Models** (9 total):
   - 4 staging models with deduplication and data quality flags
   - 5 mart models (3 dimensions + 2 facts)
   - 30+ tests with 96% pass rate (1 expected failure for fraud detection)

2. **Lightdash Dashboard** (3 charts):
   - Revenue trends for top 5 publishers
   - Fill rate performance analysis
   - Click fraud risk assessment

3. **Documentation**:
   - Comprehensive DESIGN.md with technical decisions and fraud analysis
   - EXPLORATION_NOTES.md with detailed data investigation
   - Full test coverage and data lineage

### 🚨 Critical Findings

**Click Fraud Detected**: Publisher "Pocket Gamer" shows 644% CTR (161 clicks on 25 impressions) - mathematically impossible.
- **Estimated fraud cost**: $10K+/month ($120K/year)
- **Action required**: Immediate suspension and investigation
- **7 additional high-risk publishers** identified with CTR 10-40%

**Other Data Quality Issues**:
- 2,000 duplicate events (1.52%) - resolved via deduplication
- 151 events with negative revenue (-$214 total) - flagged for investigation
- 13.9% unfilled impressions - normal for programmatic advertising

### 🏗️ Architecture
```
ClickHouse (raw data)
    ↓
dbt Staging Layer (deduplication, data quality flags)
    ↓
dbt Marts Layer (business logic, aggregations)
    ↓
Lightdash Dashboard (executive insights)
```

### 📂 Repository Structure
```
venatus-analytics-challenge/
├── dbt/
│   ├── models/
│   │   ├── staging/ad_platform/    # 4 staging models
│   │   └── marts/core/             # 5 mart models
│   └── tests/                      # Custom fraud detection test
├── DESIGN.md                       # Technical design document
├── EXPLORATION_NOTES.md            # Data investigation findings
└── README.md                       # This file
```

### 🚀 Quick Start
```bash
# Start all services
make up

# Run dbt models
make dbt-run

# Run tests
make dbt-test

# View Lightdash dashboard
open http://localhost:8880
```

### 📈 Dashboard Access

**Lightdash**: http://localhost:8880  
**Login**: admin@lightdash.com / admin123!  
**ClickHouse**: http://localhost:8123/play

### 📋 Test Results

- **Total tests**: 30
- **Passing**: 29 (96.7%)
- **Expected failures**: 1 (fraud detection alert)


# Analytics Engineering Take-Home Challenge

## Overview

You are joining the data team at **Venatus**, a programmatic advertising company. Raw data from our ad-serving platform has been pre-loaded into a **ClickHouse** data warehouse. Your task is to:

1. Build a clean, tested, and documented analytics layer using **dbt**.
2. Create a dashboard in **Lightdash** to surface key business insights.
3. Investigate the data and document what you find.

This exercise is designed to assess the core skills of an analytics engineer: data modeling, transformation, testing, documentation, and communicating with data.

---

## The Stack

| Tool | Purpose | Access |
|------|---------|--------|
| **ClickHouse** | Columnar data warehouse (raw data pre-loaded) | `http://localhost:8123/play` |
| **dbt** (dbt-clickhouse) | Data transformation framework | Runs via Docker |
| **Lightdash** | Open-source BI tool (dbt-native) | `http://localhost:8880` |

Everything runs in Docker. One command to start.

---

## Getting Started

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) (v2+)
- `make` (standard on macOS / Linux)
- Git

### 1. Clone and start

```bash
git clone git@github.com:Venatus/analytics-coding-challenge.git
cd analytics-coding-challenge
make up
```

`make up` now also runs an automatic dbt bootstrap (`deps`, `run`, `compile`) after services come up.
Wait ~60 seconds for all services to initialise and bootstrap. You'll see the URLs printed in the terminal.

### 2. Verify the raw data

Open the ClickHouse play UI at [http://localhost:8123/play](http://localhost:8123/play) and run:

```sql
SELECT count(*) FROM raw.ad_events;
-- Should return ~131,000 rows

SELECT * FROM raw.publishers LIMIT 5;
SELECT * FROM raw.campaigns  LIMIT 5;
SELECT * FROM raw.ad_units   LIMIT 5;
```

You could also connect [DBeaver](https://dbeaver.io) to Clickhouse to use a UI database manager.

### 3. Start building

If you change models after startup, rerun dbt commands manually:

```bash
make dbt-deps     # Install dbt packages (dbt-utils, etc.)
make dbt-run      # Run your models (once you've written them)
make dbt-test     # Run your tests
make dbt-compile  # Compile project (generates manifest.json for Lightdash)
```

### 4. Connect Lightdash

Lightdash is pre-configured with an account and a project connected to ClickHouse. Just log in:

- **URL**: [http://localhost:8880](http://localhost:8880)
- **Email**: `admin@lightdash.com`
- **Password**: `admin123!`

After writing your dbt models, run `make dbt-compile` then sync the project in Lightdash (Settings → Project → Syncing) so it can discover your models.

At startup, a single neutral starter dashboard/chart is auto-seeded so the Lightdash project is visibly functional. It is intentionally unrelated to the challenge questions.

---

## Raw Data

The following tables are pre-loaded in the `raw` database:

### `raw.ad_events`
Ad-serving events (impressions, clicks, viewable impressions) over ~30 days.

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | String | Event identifier |
| `event_type` | String | `impression`, `click`, `viewable_impression` |
| `event_timestamp` | DateTime64(3) | When the event occurred |
| `publisher_id` | UInt32 | Publisher (website) |
| `site_domain` | String | Domain where ad was served |
| `ad_unit_id` | String | Ad placement identifier |
| `campaign_id` | Nullable(UInt32) | Advertiser campaign — **NULL = unfilled** |
| `advertiser_id` | Nullable(UInt32) | Advertiser identifier |
| `device_type` | String | `desktop`, `mobile`, `tablet`, `ctv` |
| `country_code` | String | Two-letter country code |
| `browser` | String | Browser name |
| `revenue_usd` | Decimal64(6) | Revenue in USD |
| `bid_floor_usd` | Decimal64(6) | Minimum bid price |
| `is_filled` | UInt8 | 1 = ad was served, 0 = unfilled |
| `_loaded_at` | DateTime | When the ETL pipeline loaded this row |

### `raw.publishers`
Publisher / website dimension data (20 publishers).

### `raw.campaigns`
Advertiser campaign dimension data (27 campaigns).

### `raw.ad_units`
Ad unit / placement configuration (60 ad units across all publishers).

> **Note:** Treat this data as you would data from a production system. Explore it thoroughly before modeling — not all of it is clean.

---

## Your Tasks

### Part 1: Data Modeling with dbt (60%)

Build a dbt project that transforms the raw data into a clean, reliable analytics layer.

**Required staging models** (`models/staging/`):
- `stg_ad_events` — Deduplicated, cleaned ad events
- `stg_publishers` — Cleaned publisher dimension
- `stg_campaigns` — Cleaned campaign dimension

**Required mart models** (`models/marts/`):
- `dim_publishers` — Publisher dimension table
- `fct_ad_events_daily` — Daily aggregated ad metrics at the grain of your choosing (document it)
  - **Required metrics:** `impressions`, `clicks`, `revenue_usd`, `fill_rate`

**Nice to have** (if time permits):
- `stg_ad_units`, `dim_campaigns`, `dim_ad_units`
- Additional metrics: `viewable_impressions`, `ctr`, `viewability_rate`
- `fct_publisher_performance` — Publisher-level daily summary

**Expectations:**
- Follow dbt best practices: `source()`, `ref()`, staging → marts pattern, consistent naming
- Handle data quality issues you discover — and document your decisions
- Write **tests** in `schema.yml` files:
  - `unique` and `not_null` tests on primary / surrogate keys
  - At least one other test type (`accepted_values`, `relationships`, or a custom singular test)
- Write **documentation**:
  - Descriptions for all models and key columns
  - Document any assumptions or business logic applied

### Part 2: Dashboard in Lightdash (25%)

Connect your dbt project to Lightdash and build a dashboard that answers these business questions:

1. **Revenue Overview** — Total revenue over time, broken down by publisher.
2. **Fill Rate Analysis** — What is the fill rate by publisher? Any publishers that stand out?
3. **One insight of your choice** — What else is interesting or concerning in this data?


### Part 3: Design Document (15%)

Write a `DESIGN.md` in the repo root covering:

1. **Your data modeling approach** — Why did you structure it this way?
2. **Data quality issues** — What did you find? How did you handle each?
3. **Trade-offs** — What shortcuts did you take? What would you change with more time?
4. **Production readiness** — How would you extend this for a real production deployment?

---

## Investigation Task

While exploring the data, you should notice some **anomalous patterns** that suggests a data quality or business integrity issue.

Document what you find, **why it matters** from a business perspective, and **how you would handle it** in a production analytics pipeline.

> *Hint: Not all traffic is created equal.*

---

## Deliverables

| # | Deliverable | Format |
|---|------------|--------|
| 1 | Working dbt project | Models, tests, docs in `dbt/` |
| 2 | Lightdash dashboard | Dashboard artifact/code committed in repo (plus screenshots in `DESIGN.md` if helpful) |
| 3 | Design document | `DESIGN.md` in repo root |

> We will ask you to walk us through your solution in a follow-up interview.

---

## How We'll Run It

```bash
make up           # Start all services
make dbt-deps     # Install dbt packages
make dbt-run      # Run all models
make dbt-test     # Run all tests
```

Then we'll check:
- Models build without errors
- Tests pass
- Tables exist in ClickHouse `analytics` database and contain rows
- Lightdash dashboard is functional
- `DESIGN.md` is thoughtful and complete

---

## Evaluation Criteria

| Criteria | Weight | What we're looking for |
|----------|--------|----------------------|
| **Data Modeling** | 30% | Correct grain, clean staging → marts separation, appropriate column types and naming |
| **dbt Best Practices** | 20% | Proper use of sources, refs, tests, docs, config; consistent naming conventions |
| **Data Quality Handling** | 15% | Identifying issues in raw data, appropriate handling in staging, clear documentation |
| **Dashboard & Analysis** | 25% | Insightful visualisations that answer the business questions clearly |
| **Communication** | 10% | Clear design doc, meaningful commit messages, coherent video walkthrough |

---

## Time Expectation

**2–4 hours.** Focus on quality over quantity. A well-modeled subset with thorough tests and documentation is better than a sprawling project with no tests.

---

## Important Notes

- **Commit frequently** with meaningful, descriptive commit messages. We want to see your iterative thought process — not a single "final" commit.
- **Document your assumptions.** When the data or requirements are ambiguous, state your assumption and proceed.
- The raw data contains **intentional data quality issues**. Finding and handling them is a core part of this exercise.
- You may use any dbt packages you find helpful (e.g., `dbt-utils`, `dbt-expectations`).
- If you get stuck on infrastructure, document the issue and focus on the modeling work.
- **Do not modify** `clickhouse/init-db.sql` or `docker-compose.yml` (unless fixing a genuine bug — document it if you do).

---

## Available Commands

```
make up               # Start all services
make down             # Stop all services
make restart          # Restart all services
make logs             # Follow service logs

make dbt-deps         # Install dbt packages
make dbt-run          # Run all dbt models
make dbt-test         # Run all dbt tests
make dbt-compile      # Compile dbt project (for Lightdash)
make dbt-docs         # Generate dbt documentation
make dbt-shell        # Open a bash shell in the dbt container

make clickhouse-client  # Open ClickHouse CLI
make clean              # Remove all volumes and start fresh
```

---
