
# Reed Job Pipeline

An end-to-end ETL project that ingests job postings from the Reed.co.uk API, transforms and enriches them with Python, loads into PostgreSQL with dimensional modeling, and surfaces insights via Power BI/Fabric.

## Project Overview

This is a production-grade data pipeline demonstrating **ETL design**, **API integration**, **data warehousing**, and **analytics engineering** best practices.

### Key Capabilities

- **API Integration**: Robust Reed API client with 4-tier key rotation, rate-limit handling, and adaptive retry logic
- **Incremental Ingestion**: Configurable incremental fetching (only jobs posted in last N days) to minimize API calls
- **Smart Description Enrichment**: Auto-fetches full job descriptions from detail endpoint to work around Reed's 453-char truncation
- **Intelligent Filtering**: Rule-based title inclusion/exclusion with word-boundary matching; extensible to ML classification
- **Skill Extraction**: Extracts 60+ canonical skills from descriptions with category tagging (programming languages, databases, cloud/DevOps, ML, BI, methodologies, tools)
- **Skill Normalization**: Maps variations (e.g., `postgres` → `postgresql`, `k8s` → `kubernetes`) to canonical names
- **Salary Annualization**: Standardizes salary by type (weekly × 52, daily × 260, hourly × 1950) into consistent annual figures
- **Seniority & Location Detection**: Infers from title/description keywords
- **Employment Classification**: Extracts full-time, part-time, and contract type
- **Dimensional Schema**: Dynamic salary bands, employer/location/source dimensions for rich analytics
- **Data Quality**: Deduplication, expiry management, description validation, truncation detection
- **Materialized Analytics Table**: Pre-computed fact table with dimensional keys for optimized Power BI queries

## Architecture

```
┌─────────────────┐
│  Reed.co.uk API │
└────────┬────────┘
         │ fetch (pages with pagination & rate-limit handling)
         ▼
┌─────────────────────────────────┐
│  landing.raw_jobs (JSONB raw)   │
│  Content hash, posted/expires    │
└────────┬────────────────────────┘
         │ transform & enrich
         ▼
┌──────────────────────────────────┐
│  staging.jobs_v1 (flattened)     │
│  Extracted skills, metadata      │
└────────┬───────────────────────┬─┘
         │                       │
         ▼                       ▼
┌──────────────────┐  ┌────────────────────┐
│ staging.fact_jobs│  │ staging.job_skills │
│ Materialized w/  │  │ Skills + categories│
│ dimensional keys │  └────────────────────┘
└────────┬─────────┘
         │
         ▼
┌─────────────────┐
│ Power BI/Fabric │
│   Analytics     │
└─────────────────┘
```

## Configuration

All settings configured via `local.settings.json`:

```json
{
  "Values": {
    "API_KEY": "your-reed-api-key",
    "SEARCH_KEYWORDS": "data,bi,analyst,microsoft fabric",
    "RESULTS_PER_PAGE": 100,
    "POSTED_BY_DAYS": 30,
    "MAX_RESULTS": 0,
    "JOB_TITLE_INCLUDE": "data,bi,analyst,microsoft fabric",
    "JOB_TITLE_EXCLUDE": "trainee,intern,apprentice,asbestos,cabling,...",
    "PGHOST": "your-postgres-server",
    "PGDATABASE": "jobs_warehouse",
    "PGUSER": "admin",
    "PGPASSWORD": "****"
  }
}
```

## Running the Pipeline

### Local Test Run
```bash
python run_pipeline.py
```

### Azure Function (Timer Trigger)
Deployed as Azure Function with configurable timer schedule (e.g., daily at 9 AM).

## Project Structure

```
jobs-pipeline/
├── reed_ingest/                      # Main ETL module
│   └── __init__.py                   # Full pipeline: API, transform, enrich, load
├── sql/                              # Database schemas & dimensions
│   ├── fact_jobs.sql                 # Materialized analytics table definition
│   ├── dim_salaryband.sql            # Dynamic salary band dimension
│   └── ...                           # Other dimensional tables
├── function_app.py                   # Azure Functions entry point
├── run_pipeline.py                   # Local test runner
├── requirements.txt                  # Python dependencies
├── local.settings.json               # Environment config (gitignored)
└── README.md                         # This file
```

## Key Data Models

### Landing Layer
**`landing.raw_jobs`** - Raw API responses stored as JSONB:
- `source_name`, `job_id` (primary key)
- `raw` (complete API JSON)
- `content_hash` (for change detection)
- `posted_at`, `expires_at`

### Staging Layer
**`staging.jobs_v1`** - Flattened, transformed job records:
- All landing fields plus extracted metadata
- `salary_min`, `salary_max` (annualized)
- `work_location_type`, `seniority_level`, `job_role_category`
- `full_time`, `part_time`, `contract_type`, `salary_type`

**`staging.job_skills`** - Job-to-skill mappings:
- `source_name`, `job_id`, `skill` (canonical)
- `category` (programming_languages, databases, cloud_devops, data_ml, bi_analytics, methodologies, other_tools)

### Analytics Layer
**`staging.fact_jobs`** - Materialized denormalized table (indexed):
- All job dimensions pre-joined (`employer_key`, `location_key`, `seniority_key`, etc.)
- `salary_min/max/average` (annualized)
- `salary_min_old/max_old/average_old` (original values for audit trail)
- Salary band assignment via `salaryband_key`
- Computed fields: `days_open`, `apps_per_day`, `is_active`, `is_low_competition_ending_soon`

**Dimensional Tables**:
- `dim_salaryband` - Dynamic salary bands (£0–9,999, £10k–19,999, ..., capped at £540k–549,999)
- `dim_employer`, `dim_location`, `dim_source`, `dim_seniority`, `dim_contract`, `dim_salarytype`
- `dim_ageband` - Job age bands for analysis
- `dim_demandband` - Application volume bands
- `dim_jobtype` - Role categories (Engineering, Analyst, Scientist, Architect)

## Performance & Optimization

- **Indexed Queries**: Primary key + salary band index for fast dimension lookups
- **Materialized Table**: Pre-computed `fact_jobs` eliminates expensive joins at query time
- **Bulk Insert**: Atomic UPSERT with page-size batching (1000 rows/batch)
- **Dynamic Salary Bands**: Derived from actual data distribution, preventing gaps
- **Capped Ranges**: Salary bands capped at £540k to prevent outliers
- **Preserved Audit Trail**: `*_old` columns backfilled for historical reference

## Development & Troubleshooting

### Update Filters Without API Call
```bash
# Modify JOB_TITLE_EXCLUDE in local.settings.json, then:
python -c "
import os, json
from pathlib import Path
os.environ.update(json.loads(Path('local.settings.json').read_text())['Values'])
from reed_ingest import load_config, pg_connect, upsert_staging_jobs
cfg = load_config()
with pg_connect(cfg) as conn:
    upsert_staging_jobs(conn, include_terms=cfg['JOB_TITLE_INCLUDE'], exclude_terms=cfg['JOB_TITLE_EXCLUDE'])
"
```

### Refresh Analytics Table
```bash
python -c "
import json
from pathlib import Path
import psycopg2
settings = json.loads(Path('local.settings.json').read_text())['Values']
conn = psycopg2.connect(...)
# Rebuild staging.fact_jobs from staging.fact_jobs_old view
# See reed_ingest/__init__.py for full logic
"
```

## Technical Stack

- **Language**: Python 3.x
- **Database**: PostgreSQL (Azure Database for PostgreSQL)
- **API Client**: `requests` library with HTTP Basic Auth
- **Deployment**: Azure Functions (Timer Trigger)
- **Visualization**: Power BI / Microsoft Fabric

## Key Lessons & Achievements

✅ **Robust API Integration**: 4-key rotation, rate-limit handling, exponential backoff  
✅ **Clean Data Transformation**: Skill extraction, salary normalization, seniority detection  
✅ **Dimensional Analytics Schema**: Pre-computed facts, rich dimensions for fast BI queries  
✅ **Data Quality**: Deduplication, truncation detection, expiry cleanup  
✅ **Flexible Configuration**: Environment-driven design, rapid A/B testing of filters  
✅ **Audit Trail**: Preserved original values alongside computed fields  

## Future Enhancements

- [ ] Multi-source support (Indeed, LinkedIn, etc.)
- [ ] ML-based job classifier integration (optional, fallback to rule-based)
- [ ] Skill trend analysis and demand forecasting
- [ ] Employer/location market analysis dashboards
- [ ] Real-time Kafka ingestion for high-volume sources

### 4. **Skill Extraction Reporting**
```
🎯 Skill extraction: Unique skills=82, Jobs with skills=2311, Total matches=5325
```

## Maintenance

### Manual Duplicate Detection & Cleanup

```bash
# Detection only (read-only)
python3 cleanup_duplicates.py

# Detection + cleanup (interactive)
python3 cleanup_duplicates.py --cleanup
```

See [`duplication_prevention.md`](./docs/duplication_prevention.md) for detailed information about:
- Duplicate prevention architecture
- Root cause analysis of historical issues
- Safeguards and monitoring
- Best practices for developers

## Database Schema

### Landing Layer
- `landing.raw_jobs` - Raw JSON from Reed API search endpoint

### Staging Layer
- `staging.jobs_v1` - Flattened job records with extracted features
- `staging.job_skills` - Job-to-skill mappings with contextual metadata
- Various dimension views for analytics (dim_employer, dim_location, etc.)

### Core Layer
- `core.fact_job_posting` - Denormalized fact table for BI reporting
- `core.dim_company` - Company/employer dimension
- `core.dim_location` - Location dimension

## Configuration

Set environment variables in `local.settings.json`:

```json
{
  "Values": {
    "API_KEY": "your-reed-api-key",
    "PGHOST": "your-postgres-host",
    "PGDATABASE": "your-database",
    "PGUSER": "your-user",
    "PGPASSWORD": "your-password",
    "POSTED_BY_DAYS": "1",
    "MAX_RESULTS": "10000",
    "USE_ML_CLASSIFIER": "true",
    "ML_CLASSIFIER_THRESHOLD": "0.7"
  }
}
```

## Performance

| Metric | Value |
|--------|-------|
| Fetch 10,000 jobs | ~52 minutes |
| Skill extraction | ~45 seconds for 2,238 jobs |
| Database bulk insert | ~1 second for 2,238 rows |
| Duplicate detection | ~2 seconds |

## Status

✅ **Production Ready**
- All tables clean (0 duplicates)
- 99.7% enrichment rate
- Atomic upsert operations
- Automatic monitoring and alerts

Last updated: 2025-12-08

