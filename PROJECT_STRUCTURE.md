# Project Structure Overview

## Quick Navigation

```
jobs-pipeline/
├── README.md                        ← Start here: Architecture, setup, features
├── CHANGES.md                       ← Recent improvements & data quality work
├── DUPLICATE_PREVENTION.md          ← Data quality safeguards & troubleshooting
├── PROJECT_STRUCTURE.md             ← This file
│
├── run_pipeline.py                  ← Local test runner (main entry point for development)
├── function_app.py                  ← Azure Functions entry point (deployment)
├── requirements.txt                 ← Python dependencies
│
├── reed_ingest/                     ← Main ETL module (2,490+ lines)
│   └── __init__.py                  ├─ API client with 4-key rotation
│                                    ├─ Skill extraction & categorization
│                                    ├─ Title filtering (include/exclude rules)
│                                    ├─ Salary annualization logic
│                                    ├─ Seniority/location/employment detection
│                                    ├─ Data quality monitoring
│                                    └─ Database UPSERT operations
│
├── sql/                             ← Database schemas
│   ├── dim_salaryband.sql           ├─ Dynamic salary bands (£0–540k, 10k width)
│   ├── fact_jobs.sql                ├─ Materialized analytics table with dimensional keys
│   └── ...                          └─ Other dimension tables (location, employer, etc.)
│
├── docs/                            ← Infrastructure & architecture docs
├── powerbi/                         ← Power BI/Fabric report templates
├── infrastructure/                  ← Infrastructure-as-code (Terraform, etc.)
│
├── .gitignore                       ← Excludes secrets, local files, logs
├── host.json                        ← Azure Functions config
└── local.settings.json              ← Local environment variables (NOT committed)
```

## Key Files Explained

### Development Entry Points

| File | Purpose | When to Use |
|------|---------|-------------|
| `run_pipeline.py` | Local test runner | Testing locally before deployment |
| `function_app.py` | Azure Function trigger | Auto-deployed to Azure; timer-triggered |
| `reed_ingest/__init__.py` | Main ETL logic | Core pipeline logic (API, transform, load) |

### Documentation

| File | Purpose |
|------|---------|
| `README.md` | Project overview, features, architecture, setup |
| `CHANGES.md` | Recent improvements & what was done |
| `DUPLICATE_PREVENTION.md` | Data quality strategies & troubleshooting |
| `PROJECT_STRUCTURE.md` | This file—explains the repo layout |

### Configuration & Dependencies

| File | Purpose |
|------|---------|
| `local.settings.json` | Local dev settings (not committed; contains API keys, DB credentials) |
| `requirements.txt` | Python package dependencies |
| `host.json` | Azure Functions runtime config |
| `.gitignore` | Prevents accidental commit of secrets, logs, local files |

### Database

| Directory | Purpose |
|-----------|---------|
| `sql/` | SQL schemas for landing, staging, analytics layers |

## Data Flow

```
┌──────────────────┐
│  Reed.co.uk API  │
└────────┬─────────┘
         │ (fetch pages with pagination, rate-limit handling)
         ▼
┌─────────────────────────────┐
│ landing.raw_jobs (raw JSON) │ ← Raw API responses
└────────┬────────────────────┘
         │ (extract, transform, enrich)
         ▼
┌──────────────────────────────┐
│ staging.jobs_v1 (flattened)  │ ← Extracted skills, metadata, annualized salary
└────┬──────────────────────┬──┘
     │                      │
     ▼                      ▼
┌──────────────────┐  ┌─────────────────────┐
│ fact_jobs        │  │ job_skills (j2m)    │
│ (materialized)   │  │ (junction table)     │
└────────┬─────────┘  └─────────────────────┘
         │
         ▼
┌──────────────────────┐
│ Power BI / Fabric    │
│ (visualizations)     │
└──────────────────────┘
```

## Common Tasks

### Update Title Filters Without Re-ingesting
1. Edit `local.settings.json` → `JOB_TITLE_EXCLUDE` or `JOB_TITLE_INCLUDE`
2. Run: `python run_pipeline.py` (uses existing landing data)
3. Existing jobs re-transformed and re-scored without API calls

### Deploy to Azure
1. Ensure `requirements.txt` and `local.settings.json` (with secrets) are ready
2. Azure CLI: `func azure functionapp publish <function-name>`
3. Verify timer trigger in Azure Portal

### Check Data Quality
Duplicate detection and enrichment stats are logged at end of each pipeline run.

### Add New Dimension or Change Salary Bands
1. Update SQL in `sql/dim_salaryband.sql` (or create new dimension)
2. Apply in PostgreSQL
3. Run pipeline to refresh materialized `fact_jobs` with new keys

## Dependencies

**Python Packages** (see `requirements.txt`):
- `psycopg2` – PostgreSQL adapter
- `requests` – HTTP client for Reed API
- `azure-functions` – Azure Functions SDK

**External Services**:
- Reed.co.uk API (requires API key)
- PostgreSQL database (Azure Database for PostgreSQL)
- Azure Functions (for scheduled deployment)
- Power BI / Microsoft Fabric (for visualization)

## Troubleshooting

**Pipeline fails on local run?**
- Check `local.settings.json` exists and has valid credentials
- Verify PostgreSQL is reachable: `psql -h <host> -U <user> -d <database>`

**Data not flowing to staging?**
- Check for title filter mismatches (word boundaries are strict)
- Enable verbose logging in `reed_ingest/__init__.py`

**Duplicates detected?**
- See `DUPLICATE_PREVENTION.md` for detailed troubleshooting
- Common cause: individual INSERT loops (should use batch UPSERT)

**Azure deployment issues?**
- Verify `local.settings.json` secrets are in Azure Key Vault
- Check Azure Function runtime logs in Portal
- Confirm timer trigger schedule in `function_app.py`

## Next Steps

1. **Deploy**: Push to Azure Functions with `func azure functionapp publish`
2. **Monitor**: Set up Application Insights alerts in Azure Portal
3. **Visualize**: Build Power BI dashboards connecting to `staging.fact_jobs`
4. **Expand**: Add additional job sources (Indeed, LinkedIn) or job categories
