"""
Reed Job Pipeline ETL Module

This module implements a complete ETL (Extract-Transform-Load) pipeline for ingesting
job postings from the Reed.co.uk API, extracting skills, and storing enriched data
in PostgreSQL with contextual metadata.

Data Flow:
  1. API Fetch (Reed API search endpoint) -> landing.raw_jobs
  2. Transform & Enrich -> staging.jobs_v1 (with skill extraction & classification)
  3. Skill Normalization -> staging.job_skills (with context: importance, proficiency)
  4. Analytics Views -> mart schema (v_job_skill_counts, v_job_category_coverage)

Key Features:
  • Incremental fetching (POSTED_BY_DAYS parameter)
  • Description enrichment (full text via /jobs/{id} detail endpoint)
  • Skill extraction with contextual importance/proficiency detection
  • Work location classification (remote/hybrid/office)
  • Seniority level detection (executive/director/manager/lead/senior/mid/junior/entry)
  • Expired job cleanup & enrichment statistics logging
  • Change detection via content_hash for efficient updates

Configuration: All settings via local.settings.json (environment variables)
Logging: Structured output with emoji indicators for quick scanning

Author: Graeme Boulton
Last Updated: 2025-12-04
"""

import os
import sys
import re
import time
from pathlib import Path

# Add current directory to path to import job_classifier
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import json
import math
import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import azure.functions as func
import requests
import psycopg2
import psycopg2.extras

# Import ML classifier
try:
    from job_classifier import JobClassifier
    ML_CLASSIFIER_AVAILABLE = True
except ImportError:
    ML_CLASSIFIER_AVAILABLE = False
    print("⚠️  ML classifier not available - will use rule-based filtering only")


# ---------- Skills extraction patterns ----------

# Grouped skills to enable category tagging (e.g., programming_languages, databases)
SKILL_GROUPS: Dict[str, set] = {
    "programming_languages": {
        'python', 'java', 'r', 'scala', 'SQL'
        # Removed: 'c#', 'go', 'r' (too short / high false-positive rate)
    },
    "databases": {
        'sql', 'mysql', 'postgresql', 'postgres', 'mongodb', 'oracle', 'sql server', 'dynamodb',
        'Parquet', 'snowflake', 'bigquery'
    },
    "cloud_devops": {
        'aws', 'azure', 'gcp', 'google cloud', 'Data Factory', 'Synapse', 'terraform', 'ansible', 'jenkins', 'gitlab',
        'S3', 'Glue', 'Lambda', 'Redshift', 'Athena', 'Fabric'
        # Removed: 'k8s' (< 3 chars)
    },
    "data_ml": {
        'apache spark', 'dbt', 'tensorflow', 'pytorch', 'scikit-learn',
        'pandas', 'numpy', 'matplotlib', 'jupyter', 'spark', 'hadoop', 'kafka', 'airflow', 'databricks', 'mlflow'
        # Removed: 'ai' (too short / high false-positive rate)
    },
    "bi_analytics": {
        'tableau', 'power bi', 'looker', 'qlik', 'excel'
    },
    "other_tools": {
        'gi hub', 'jira', 'git', 'bash', 'powershell', 'vim', 'ci/cd', 'intellij',
        'selenium', 'cypress', 'jest', 'pytest', 'junit', 'postman'
    }
}

# Flat set used for matching (backwards-compatible)
SKILL_PATTERNS = set().union(*SKILL_GROUPS.values())

# Job role categories - map role keywords to standardized category names
# Format: 'category_name': {'keywords', 'to', 'match'}
JOB_ROLE_CATEGORIES: Dict[str, set] = {
    'Engineering': {'engineer', 'engineering', 'developer', 'devops', 'infrastructure', 'platform'},
    'Analyst': {'analyst', 'analytics', 'business intelligence', 'bi', 'data analyst'},
    'Scientist': {'scientist', 'research', 'machine learning', 'ml engineer'},
    'Architect': {'architect', 'architecture'},
}

# Map skill variations to canonical names
# Format: 'variation': 'canonical_name'
SKILL_ALIASES = {
    # Power BI variations
    'powerbi': 'power bi',
    'power-bi': 'power bi',
    'ms power bi': 'power bi',
    'microsoft power bi': 'power bi',
    
    # PostgreSQL variations
    'postgres': 'postgresql',
    'postgre': 'postgresql',
    
    # Kubernetes variations
    'k8s': 'kubernetes',
    'k8': 'kubernetes',

    
    # JavaScript variations
    'js': 'javascript',
    'java script': 'javascript',
    
    # CI/CD variations
    'ci cd': 'ci/cd',
    'cicd': 'ci/cd',
    'continuous integration': 'ci/cd',
    'continuous deployment': 'ci/cd',
    
    # Data Science variations
    'data-science': 'data science',
    'datascience': 'data science',
    
    # Data Analysis variations
    'data-analysis': 'data analysis',
    'dataanalysis': 'data analysis',
}


# ---------- Env helpers ----------

def _must_get(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def load_config() -> Dict[str, Any]:
    # Azure Functions / general
    cfg = {
        "AZURE_WEBJOBS_STORAGE": os.getenv("AzureWebJobsStorage", ""),
        "FUNCTIONS_WORKER_RUNTIME": os.getenv("FUNCTIONS_WORKER_RUNTIME", "python"),
    }

    # Reed API basics
    cfg["API_KEY"] = _must_get("API_KEY")
    cfg["API_KEY_BACKUP"] = os.getenv("API_KEY_BACKUP", "")  # Fallback key for rate limit handling
    cfg["API_BASE_URL"] = _must_get("API_BASE_URL")
    cfg["SEARCH_KEYWORDS"] = os.getenv("SEARCH_KEYWORDS", "data")
    cfg["RESULTS_PER_PAGE"] = int(os.getenv("RESULTS_PER_PAGE", "50"))
    cfg["MAX_RESULTS"] = int(os.getenv("MAX_RESULTS", "0"))  # 0 = unlimited; >0 = cap at this number
    cfg["SOURCE_NAME"] = os.getenv("SOURCE_NAME", "reed")
    cfg["POSTED_BY_DAYS"] = int(os.getenv("POSTED_BY_DAYS", "1"))  # Incremental: only fetch jobs posted in last N days

    # Field names coming back from the API (make them overrideable)
    cfg["RESULTS_KEY"] = os.getenv("RESULTS_KEY", "results")
    cfg["TOTAL_PAGES_KEY"] = os.getenv("TOTAL_PAGES_KEY", "totalResults")
    cfg["JOB_ID_KEY"] = os.getenv("JOB_ID_KEY", "jobId")
    cfg["POSTED_DATE_KEY"] = os.getenv("POSTED_DATE_KEY", "datePosted")
    cfg["EXPIRY_DATE_KEY"] = os.getenv("EXPIRY_DATE_KEY", "expirationDate")
    cfg["DATE_MODIFIED_KEY"] = os.getenv("DATE_MODIFIED_KEY", "dateModified")

    # Postgres (split vars)
    cfg["PGHOST"] = _must_get("PGHOST")
    cfg["PGPORT"] = int(os.getenv("PGPORT", "5432"))
    cfg["PGDATABASE"] = _must_get("PGDATABASE")
    cfg["PGUSER"] = _must_get("PGUSER")
    cfg["PGPASSWORD"] = _must_get("PGPASSWORD")
    cfg["PGSSLMODE"] = os.getenv("PGSSLMODE", "require")

    # Title filters (include/exclude, comma-separated; applied to jobTitle)
    include_str = os.getenv("JOB_TITLE_INCLUDE", "data,bi,analyst,fabric")
    exclude_str = os.getenv("JOB_TITLE_EXCLUDE", "trainee,intern,apprentice,asbestos,cabling,data protection,desktop support,data administrator,it support,laboratory analyst,lab analyst,lab technician,project manager,qc,qa,recruitment,test analyst")
    cfg["JOB_TITLE_INCLUDE"] = [s.strip().lower() for s in include_str.split(",") if s.strip()]
    cfg["JOB_TITLE_EXCLUDE"] = [s.strip().lower() for s in exclude_str.split(",") if s.strip()]

    # Skill extraction patterns
    # Option 1: Override entire SKILL_GROUPS with JSON (format: {"category": ["skill1", "skill2"]})
    groups_json = os.getenv("SKILL_GROUPS", "")
    if groups_json:
        try:
            user_groups = json.loads(groups_json)
            cfg["SKILL_GROUPS"] = {k: set(v) for k, v in user_groups.items()}
            cfg["SKILL_PATTERNS"] = set().union(*cfg["SKILL_GROUPS"].values())
        except json.JSONDecodeError as e:
            print(f"⚠️ SKILL_GROUPS JSON invalid, using defaults: {e}")
            cfg["SKILL_GROUPS"] = SKILL_GROUPS
            cfg["SKILL_PATTERNS"] = SKILL_PATTERNS
    else:
        # Option 2: Comma-separated skill list (backwards compatible, overrides grouped patterns)
        skills_str = os.getenv("SKILL_PATTERNS", "")
        if skills_str:
            cfg["SKILL_PATTERNS"] = set(s.strip().lower() for s in skills_str.split(",") if s.strip())
            cfg["SKILL_GROUPS"] = SKILL_GROUPS  # Keep default groups for category lookup
        else:
            # Use defaults
            cfg["SKILL_GROUPS"] = SKILL_GROUPS
            cfg["SKILL_PATTERNS"] = SKILL_PATTERNS
    
    # Skill aliases (format: "variation1->canonical1,variation2->canonical2")
    aliases_str = os.getenv("SKILL_ALIASES", "")
    if aliases_str:
        # Parse user-provided aliases
        cfg["SKILL_ALIASES"] = {}
        for mapping in aliases_str.split(","):
            if "->" in mapping:
                variation, canonical = mapping.split("->", 1)
                cfg["SKILL_ALIASES"][variation.strip().lower()] = canonical.strip().lower()
    else:
        # Use default aliases
        cfg["SKILL_ALIASES"] = SKILL_ALIASES

    # Job role categories (format: {"Category1": ["keyword1", "keyword2"], "Category2": [...]})
    role_categories_json = os.getenv("JOB_ROLE_CATEGORIES", "")
    if role_categories_json:
        try:
            user_categories = json.loads(role_categories_json)
            cfg["JOB_ROLE_CATEGORIES"] = {k: set(v) for k, v in user_categories.items()}
        except json.JSONDecodeError as e:
            print(f"⚠️ JOB_ROLE_CATEGORIES JSON invalid, using defaults: {e}")
            cfg["JOB_ROLE_CATEGORIES"] = JOB_ROLE_CATEGORIES
    else:
        cfg["JOB_ROLE_CATEGORIES"] = JOB_ROLE_CATEGORIES

    # ML Classifier settings
    cfg["USE_ML_CLASSIFIER"] = os.getenv("USE_ML_CLASSIFIER", "false").lower() == "true"
    cfg["ML_CLASSIFIER_THRESHOLD"] = float(os.getenv("ML_CLASSIFIER_THRESHOLD", "0.7"))

    return cfg


# ---------- DB helpers ----------

def pg_connect(cfg: Dict[str, Any]):
    return psycopg2.connect(
        host=cfg["PGHOST"],
        port=cfg["PGPORT"],
        dbname=cfg["PGDATABASE"],
        user=cfg["PGUSER"],
        password=cfg["PGPASSWORD"],
        sslmode=cfg["PGSSLMODE"],
    )


def ensure_landing_table(conn):
    sql = """
    CREATE SCHEMA IF NOT EXISTS landing;

    CREATE TABLE IF NOT EXISTS landing.raw_jobs (
        source_name     TEXT        NOT NULL,
        job_id          TEXT        NOT NULL,
        title           TEXT,
        employer        TEXT,
        location        TEXT,
        posted_at       TIMESTAMPTZ,
        expires_at      TIMESTAMPTZ,
        date_modified   TIMESTAMPTZ,
        raw             JSONB       NOT NULL,
        content_hash    TEXT        NOT NULL,
        ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (source_name, job_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def ensure_staging_table(conn):
    sql = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.jobs_v1 (
        staging_id      BIGSERIAL   PRIMARY KEY,
        source_name     TEXT        NOT NULL,
        job_id          TEXT        NOT NULL,
        job_title       TEXT,
        employer_name   TEXT,
        employer_id     BIGINT,
        location_name   TEXT,
        salary_min      NUMERIC,
        salary_max      NUMERIC,
        job_url         TEXT,
        applications    INT,
        job_description TEXT,
        posted_at       TIMESTAMPTZ,
        expires_at      TIMESTAMPTZ,
        ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        work_location_type TEXT,
        seniority_level TEXT,
        job_role_category TEXT,
        contract_type   TEXT,
        full_time       BOOLEAN,
        part_time       BOOLEAN,
        salary_type     TEXT,
        UNIQUE (source_name, job_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        # Ensure columns exist if table was created previously
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS work_location_type TEXT;")
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS seniority_level TEXT;")
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS job_role_category TEXT;")
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS contract_type TEXT;")
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS full_time BOOLEAN;")
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS part_time BOOLEAN;")
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS salary_type TEXT;")
    conn.commit()


def ensure_job_skills_table(conn):
    sql = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.job_skills (
        id              BIGSERIAL   PRIMARY KEY,
        source_name     TEXT        NOT NULL,
        job_id          TEXT        NOT NULL,
        category        TEXT,
        skill           TEXT        NOT NULL,   -- canonical/normalized skill name
        matched_pattern TEXT,                   -- the variation/pattern matched in text
        extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (source_name, job_id, skill)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        # Ensure column exists if table created previously
        cur.execute("ALTER TABLE staging.job_skills ADD COLUMN IF NOT EXISTS category TEXT;")
    conn.commit()


def upsert_jobs(conn, rows: List[Tuple]):
    """
    rows: list of tuples matching VALUES order below
    """
    sql = """
    INSERT INTO landing.raw_jobs (
        source_name, job_id, title, employer, location,
        posted_at, expires_at, date_modified, raw, content_hash
    )
    VALUES %s
    ON CONFLICT (source_name, job_id) DO UPDATE
    SET
        title = EXCLUDED.title,
        employer = EXCLUDED.employer,
        location = EXCLUDED.location,
        posted_at = EXCLUDED.posted_at,
        expires_at = EXCLUDED.expires_at,
        date_modified = EXCLUDED.date_modified,
        raw = EXCLUDED.raw,
        content_hash = EXCLUDED.content_hash,
        ingested_at = NOW();
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    conn.commit()


def upsert_staging_jobs(conn, skill_patterns: set = None, skill_aliases: dict = None, include_terms: List[str] = None, exclude_terms: List[str] = None, job_role_categories: Dict[str, set] = None):
    """
    Transform landing.raw_jobs → staging.jobs_v1
    Extracts and flattens JSON fields from raw column
    Includes skill extraction from job descriptions
    
    Args:
        conn: Database connection
        skill_patterns: Set of skills to extract (uses default if None)
        skill_aliases: Dict mapping variations to canonical names (uses default if None)
        job_role_categories: Dict mapping role category names to keyword sets (uses default if None)
    """
    # First, fetch job descriptions that need skill extraction
    # Build filtered selection of jobs from landing.raw_jobs based on title include/exclude
    # NOTE: Filtering happens in Python (not SQL) to use same word boundary logic as main()
    include_terms = include_terms or []
    exclude_terms = exclude_terms or []
    
    # Fetch all jobs from landing that have descriptions
    sel_sql = """
        SELECT job_id, source_name, raw->>'jobTitle' as title, raw->>'jobDescription' as description, raw->>'locationName' as location_name,
               (raw->>'minimumSalary')::numeric as salary_min, (raw->>'maximumSalary')::numeric as salary_max
        FROM landing.raw_jobs
        WHERE raw->>'jobDescription' IS NOT NULL AND raw->>'jobTitle' IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(sel_sql)
        all_jobs_raw = cur.fetchall()
    
    # Standardize locations and add to jobs tuple
    all_jobs = []
    location_standardization_map = {}  # Cache for location standardization
    for job_id, source_name, title, description, raw_location, api_salary_min, api_salary_max in all_jobs_raw:
        # Standardize location if not already cached
        if raw_location:
            if raw_location not in location_standardization_map:
                location_standardization_map[raw_location] = standardize_location(raw_location)
            standardized_location = location_standardization_map[raw_location]
        else:
            standardized_location = None
        
        all_jobs.append((job_id, source_name, title, description, standardized_location, api_salary_min, api_salary_max))
    
    # Store standardized locations for later use
    standardized_location_map = {}  # (source_name, job_id) -> standardized_location
    for job_id, source_name, title, description, std_location, api_salary_min, api_salary_max in all_jobs:
        standardized_location_map[(source_name, job_id)] = std_location
    
    # Filter jobs using same word boundary logic as main function
    jobs = []
    for job_id, source_name, title, description, std_location, api_salary_min, api_salary_max in all_jobs:
        if not title:
            continue
        
        t = title.lower()
        
        # Check include filter
        if include_terms:
            include_match = False
            for term in include_terms:
                pattern = r'\b' + re.escape(term) + r'\b'
                if re.search(pattern, t):
                    include_match = True
                    break
            if not include_match:
                continue
        
        # Check exclude filter
        if exclude_terms:
            exclude_match = False
            for term in exclude_terms:
                pattern = r'\b' + re.escape(term) + r'\b'
                if re.search(pattern, t):
                    exclude_match = True
                    break
            if exclude_match:
                continue
        
        jobs.append((job_id, source_name, title, description, std_location, api_salary_min, api_salary_max))
    
    # Extract skills and detect work location type for each job
    skills_map: Dict[Tuple[str, str], List[str]] = {}
    location_map: Dict[Tuple[str, str], str] = {}
    seniority_map: Dict[Tuple[str, str], str] = {}
    role_category_map: Dict[Tuple[str, str], Optional[str]] = {}
    salary_fallback_map: Dict[Tuple[str, str], Tuple[Optional[float], Optional[float]]] = {}
    salary_type_map: Dict[Tuple[str, str], Optional[str]] = {}
    employment_map: Dict[Tuple[str, str], Tuple[Optional[bool], Optional[bool], Optional[str]]] = {}
    location_name_map: Dict[Tuple[str, str], str] = {}  # For standardized city names
    # rows of (source_name, job_id, category, canonical)
    job_skill_pairs: List[Tuple[str, str, str, str]] = []
    # Build reverse map pattern -> category for fast lookup
    pattern_category: Dict[str, str] = {}
    for cat, patterns in SKILL_GROUPS.items():
        for p in patterns:
            pattern_category[p] = cat
    
    # Use provided job role categories or default
    if job_role_categories is None:
        job_role_categories = JOB_ROLE_CATEGORIES
    
    for job_id, source_name, title, description, std_location, api_sal_min, api_sal_max in jobs:
        # Extract skills using the updated extract_skills function (min length filter applied there)
        canonical_skills = extract_skills(description, skill_patterns, skill_aliases)
        skills_map[(source_name, job_id)] = canonical_skills
        # Detect work location type
        location_type = detect_work_location_type(title, description)
        location_map[(source_name, job_id)] = location_type
        # Detect seniority level
        seniority = detect_seniority_level(title, description)
        seniority_map[(source_name, job_id)] = seniority
        # Detect job role category
        role_category = detect_job_role_category(title, job_role_categories)
        role_category_map[(source_name, job_id)] = role_category
        # Parse salary from description when API fields are missing
        sal_min, sal_max, _ = parse_salary_from_text(description)
        salary_fallback_map[(source_name, job_id)] = (sal_min, sal_max)
        # Use API salary if available, otherwise parsed salary
        sal_min_for_type = api_sal_min or sal_min
        sal_max_for_type = api_sal_max or sal_max
        salary_type_inferred = detect_salary_type(title, description, sal_min_for_type, sal_max_for_type)
        salary_type_map[(source_name, job_id)] = salary_type_inferred
        # Detect employment terms (full/part time, contract type)
        employment_map[(source_name, job_id)] = detect_employment_terms(title, description)
        # Store standardized location (already standardized during fetch)
        standardized_location_map[(source_name, job_id)] = std_location
        # Accumulate job-skill rows (source_name, job_id, category, skill)
        for canonical in canonical_skills:
            cat = pattern_category.get(canonical)
            job_skill_pairs.append((source_name, job_id, cat or 'uncategorized', canonical))
    
    print(f"🔍 Extracted skills from {len(skills_map)} job descriptions")
    
    # Now upsert with skills
    sql = """
    INSERT INTO staging.jobs_v1 (
        source_name, job_id, job_title, employer_name, employer_id,
        location_name, salary_min, salary_max, currency, job_url,
        applications, job_description, extracted_skills, posted_at, expires_at,
        date_modified, ingested_at, updated_at, content_hash
    )
    SELECT 
        source_name,
        job_id,
        raw->>'jobTitle' as job_title,
        raw->>'employerName' as employer_name,
        (raw->>'employerId')::bigint as employer_id,
        raw->>'locationName' as location_name,
        (raw->>'minimumSalary')::numeric as salary_min,
        (raw->>'maximumSalary')::numeric as salary_max,
        raw->>'currency' as currency,
        raw->>'jobUrl' as job_url,
        (raw->>'applications')::int as applications,
        raw->>'jobDescription' as job_description,
        %s as extracted_skills,
        posted_at,
        expires_at,
        date_modified,
        ingested_at,
        NOW() as updated_at,
        content_hash
    FROM landing.raw_jobs
    ON CONFLICT (source_name, job_id) DO UPDATE SET
        job_title = EXCLUDED.job_title,
        employer_name = EXCLUDED.employer_name,
        employer_id = EXCLUDED.employer_id,
        location_name = EXCLUDED.location_name,
        salary_min = EXCLUDED.salary_min,
        salary_max = EXCLUDED.salary_max,
        currency = EXCLUDED.currency,
        job_url = EXCLUDED.job_url,
        applications = EXCLUDED.applications,
        job_description = EXCLUDED.job_description,
        extracted_skills = EXCLUDED.extracted_skills,
        posted_at = EXCLUDED.posted_at,
        expires_at = EXCLUDED.expires_at,
        date_modified = EXCLUDED.date_modified,
        content_hash = EXCLUDED.content_hash,
        updated_at = NOW();
    """
    
    # Bulk insert into staging.jobs_v1 using a single atomic operation
    # Build a temp table with all job data, then upsert in one go
    row_count = len(skills_map)
    
    if row_count > 0:
        with conn.cursor() as cur:
            # Create a temporary table for bulk insert
            cur.execute("""
                CREATE TEMP TABLE staging_jobs_temp (
                    source_name text,
                    job_id text,
                    salary_min numeric,
                    salary_max numeric,
                    work_location_type text,
                    seniority_level text,
                    job_role_category text,
                    contract_type text,
                    full_time boolean,
                    part_time boolean,
                    salary_type text,
                    standardized_location_name text
                ) ON COMMIT DROP
            """)
            
            # Prepare bulk data for temp table
            temp_data = []
            for (source_name, job_id), skills in skills_map.items():
                location_type = location_map.get((source_name, job_id), 'unknown')
                seniority = seniority_map.get((source_name, job_id), 'mid')
                role_category = role_category_map.get((source_name, job_id))
                salary_min, salary_max = salary_fallback_map.get((source_name, job_id), (None, None))
                full_time, part_time, contract = employment_map.get((source_name, job_id), (None, None, None))
                salary_type = salary_type_map.get((source_name, job_id))
                std_location = standardized_location_map.get((source_name, job_id))
                
                temp_data.append((
                    source_name, job_id, salary_min, salary_max,
                    location_type, seniority, role_category,
                    contract, full_time, part_time, salary_type, std_location
                ))
            
            # Bulk insert into temp table
            from psycopg2.extras import execute_values
            execute_values(cur, """
                INSERT INTO staging_jobs_temp 
                (source_name, job_id, salary_min, salary_max, work_location_type,
                 seniority_level, job_role_category, contract_type, full_time, part_time, salary_type, standardized_location_name)
                VALUES %s
            """, temp_data, page_size=1000)
            
            # Single atomic upsert from temp table to jobs_v1
            cur.execute("""
                INSERT INTO staging.jobs_v1 (
                    source_name, job_id, job_title, employer_name, employer_id,
                    location_name, salary_min, salary_max, job_url,
                    applications, job_description, work_location_type, seniority_level,
                    job_role_category, contract_type, full_time, part_time, salary_type,
                    posted_at, expires_at, ingested_at, updated_at
                )
                SELECT 
                    r.source_name,
                    r.job_id,
                    r.raw->>'jobTitle' as job_title,
                    r.raw->>'employerName' as employer_name,
                    (r.raw->>'employerId')::bigint as employer_id,
                    COALESCE(t.standardized_location_name, r.raw->>'locationName') as location_name,
                    CASE
                        WHEN t.salary_type = 'per week' THEN COALESCE((r.raw->>'minimumSalary')::numeric, t.salary_min) * 52
                        WHEN t.salary_type = 'per day' THEN COALESCE((r.raw->>'minimumSalary')::numeric, t.salary_min) * 260
                        WHEN t.salary_type = 'per hour' THEN COALESCE((r.raw->>'minimumSalary')::numeric, t.salary_min) * 1950
                        ELSE COALESCE((r.raw->>'minimumSalary')::numeric, t.salary_min)
                    END as salary_min,
                    CASE
                        WHEN t.salary_type = 'per week' THEN COALESCE((r.raw->>'maximumSalary')::numeric, t.salary_max) * 52
                        WHEN t.salary_type = 'per day' THEN COALESCE((r.raw->>'maximumSalary')::numeric, t.salary_max) * 260
                        WHEN t.salary_type = 'per hour' THEN COALESCE((r.raw->>'maximumSalary')::numeric, t.salary_max) * 1950
                        ELSE COALESCE((r.raw->>'maximumSalary')::numeric, t.salary_max)
                    END as salary_max,
                    r.raw->>'jobUrl' as job_url,
                    (r.raw->>'applications')::int as applications,
                    r.raw->>'jobDescription' as job_description,
                    t.work_location_type,
                    t.seniority_level,
                    t.job_role_category,
                    COALESCE(t.contract_type, lower(r.raw->>'contractType'), 'permanent') as contract_type,
                    COALESCE(
                        t.full_time,
                        (r.raw->>'fullTime')::boolean,
                        CASE
                            WHEN COALESCE(t.part_time, (r.raw->>'partTime')::boolean, false) = false
                                 AND COALESCE(t.contract_type, lower(r.raw->>'contractType'), 'permanent') = 'permanent'
                            THEN true
                            ELSE false
                        END
                    ) as full_time,
                    COALESCE(t.part_time, (r.raw->>'partTime')::boolean, false) as part_time,
                    t.salary_type,
                    r.posted_at,
                    r.expires_at,
                    r.ingested_at,
                    NOW() as updated_at
                FROM landing.raw_jobs r
                INNER JOIN staging_jobs_temp t ON (r.source_name = t.source_name AND r.job_id = t.job_id)
                ON CONFLICT (source_name, job_id) DO UPDATE SET
                    job_title = EXCLUDED.job_title,
                    employer_name = EXCLUDED.employer_name,
                    employer_id = EXCLUDED.employer_id,
                    location_name = EXCLUDED.location_name,
                    salary_min = EXCLUDED.salary_min,
                    salary_max = EXCLUDED.salary_max,
                    job_url = EXCLUDED.job_url,
                    applications = EXCLUDED.applications,
                    job_description = EXCLUDED.job_description,
                    work_location_type = EXCLUDED.work_location_type,
                    seniority_level = EXCLUDED.seniority_level,
                    job_role_category = EXCLUDED.job_role_category,
                    contract_type = EXCLUDED.contract_type,
                    full_time = EXCLUDED.full_time,
                    part_time = EXCLUDED.part_time,
                    salary_type = EXCLUDED.salary_type,
                    posted_at = EXCLUDED.posted_at,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = NOW()
            """)
    
    # Upsert job_skills mapping table in bulk (category, canonical skill, matched pattern)
    if job_skill_pairs:
        upsert_job_skills(conn, job_skill_pairs)

    conn.commit()
    return row_count


# ---------- API helpers ----------


def upsert_job_skills(conn, rows: List[Tuple[str, str, str, str]]):
    """
    Bulk upsert into staging.job_skills
    Rows: (source_name, job_id, category, skill (canonical))
    """
    sql = """
    INSERT INTO staging.job_skills (
        source_name, job_id, category, skill
    ) VALUES %s
    ON CONFLICT (source_name, job_id, skill) DO UPDATE
    SET category = EXCLUDED.category,
        extracted_at = NOW();
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
def basic_auth_header(api_key: str) -> Dict[str, str]:
    # Reed uses HTTP Basic with key as username, blank password.
    # requests will handle auth better via "auth=(api_key, '')", but we’ll keep explicit header for clarity.
    # However, here we’ll prefer requests.auth tuple usage inside fetch_page().
    return {"Accept": "application/json"}


def fetch_page(
    base_url: str,
    keywords: str,
    results_per_page: int,
    page: int,
    api_key: str,
    api_key_backup: Optional[str] = None,
    api_key_backup_2: Optional[str] = None,
    api_key_backup_3: Optional[str] = None,
    posted_by_days: Optional[int] = None,
    key_rotation_index: int = 0
) -> Dict[str, Any]:
    """
    Fetch single page of results from Reed API search endpoint.
    
    Supports pagination via resultsToTake/resultsToSkip parameters.
    Optionally filters to jobs posted within N days via postedByDays parameter
    (0 = all jobs, >0 = incremental mode).

    Uses round-robin key rotation (all 4 keys) to distribute rate limits evenly.
    Falls back through keys on 403 Forbidden errors (rate limiting).
    
    Args:
        base_url: Reed API base URL (https://www.reed.co.uk/api/1.0/search)
        keywords: Search keyword string (comma-separated for multiple)
        results_per_page: Page size (typically 50)
        page: Page number (1-based)
        api_key: Reed API authentication key (primary)
        api_key_backup: First backup API key for rate limit fallback (optional)
        api_key_backup_2: Second backup API key (optional)
        api_key_backup_3: Third backup API key (optional)
        posted_by_days: Filter to jobs posted in last N days (optional)
        key_rotation_index: Current rotation index for round-robin key selection
        
    Returns:
        API response dict with 'results' and 'totalResults' keys
        
    Raises:
        requests.HTTPError: On API request failure (all keys exhausted)
    """
    params = {
        "keywords": keywords,
        "resultsToTake": results_per_page,
        "resultsToSkip": (page - 1) * results_per_page,
    }
    
    # Add incremental filter: only fetch jobs posted in last N days
    if posted_by_days and posted_by_days > 0:
        params["postedByDays"] = posted_by_days
    
    # Build ordered list of keys starting from rotation index (round-robin load balancing)
    api_keys = [api_key]
    if api_key_backup:
        api_keys.append(api_key_backup)
    if api_key_backup_2:
        api_keys.append(api_key_backup_2)
    if api_key_backup_3:
        api_keys.append(api_key_backup_3)
    
    # Rotate key order: start from rotation_index to distribute load evenly
    rotated_keys = api_keys[key_rotation_index % len(api_keys):] + api_keys[:key_rotation_index % len(api_keys)]
    
    max_retries = 3
    retry_delay = 0.5
    
    for i, key in enumerate(rotated_keys):
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(base_url, params=params, auth=(key, ""))
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as e:
                # 500/502/503 errors: retry with exponential backoff
                if e.response.status_code in [500, 502, 503]:
                    if attempt < max_retries:
                        backoff = retry_delay * (2 ** (attempt - 1))
                        print(f"⚠️ API key #{(key_rotation_index + i) % len(api_keys) + 1} returned {e.response.status_code} (attempt {attempt}/{max_retries}); retrying in {backoff:.1f}s...")
                        time.sleep(backoff)
                        continue
                    else:
                        print(f"⚠️ API key #{(key_rotation_index + i) % len(api_keys) + 1} returned {e.response.status_code} after {max_retries} attempts; trying next key...")
                        break  # Move to next key
                # 403 rate limit: move to next key immediately
                elif e.response.status_code == 403 and i < len(rotated_keys) - 1:
                    print(f"⚠️ API key #{(key_rotation_index + i) % len(api_keys) + 1} rate limited (403); trying next key...")
                    break  # Move to next key
                # Other errors: try next key
                elif i < len(rotated_keys) - 1:
                    print(f"⚠️ API key #{(key_rotation_index + i) % len(api_keys) + 1} failed ({e}); trying next key...")
                    break  # Move to next key
                else:
                    raise
            except Exception as e:
                if i < len(rotated_keys) - 1:
                    print(f"⚠️ API key #{(key_rotation_index + i) % len(api_keys) + 1} error ({e}); trying next key...")
                    break  # Move to next key
                else:
                    raise
    
    # Should never reach here
    raise RuntimeError("No API keys available")


def fetch_job_detail(
    api_base_url: str,
    job_id: str,
    api_key: str,
    api_key_backup_1: Optional[str] = None,
    api_key_backup_2: Optional[str] = None,
    api_key_backup_3: Optional[str] = None,
    key_rotation_index: int = 0,
    backoff_seconds: float = 0.5
) -> Optional[Dict[str, Any]]:
    """
    Fetch full job details for a given job_id using Reed API.
    Tries the jobs endpoint inferred from API_BASE_URL.
    Falls back through four API keys on failure (403 or other errors).
    
    Args:
        api_base_url: Reed API base URL
        job_id: Job ID to fetch details for
        api_key: Primary API key
        api_key_backup_1: First backup API key (optional)
        api_key_backup_2: Second backup API key (optional)
        api_key_backup_3: Third backup API key (optional)
        
    Returns:
        Job detail dict or None if all API keys fail
    """
    # Infer jobs endpoint: replace trailing '/search' with '/jobs/{id}'
    jobs_url = api_base_url.rstrip('/')
    if jobs_url.endswith('/search'):
        jobs_url = jobs_url[:-len('/search')]
    jobs_url = f"{jobs_url}/jobs/{job_id}"
    
    # Try API keys in sequence with round-robin rotation: primary → backup_1 → backup_2 → backup_3
    api_keys = [api_key]
    if api_key_backup_1:
        api_keys.append(api_key_backup_1)
    if api_key_backup_2:
        api_keys.append(api_key_backup_2)
    if api_key_backup_3:
        api_keys.append(api_key_backup_3)

    rotated_keys = api_keys[key_rotation_index % len(api_keys):] + api_keys[:key_rotation_index % len(api_keys)]

    for i, key in enumerate(rotated_keys):
        try:
            r = requests.get(jobs_url, auth=(key, ""))
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            if e.response.status_code == 403 and i < len(rotated_keys) - 1:
                print(f"⚠️ API key #{(key_rotation_index + i)%len(api_keys)+1} rate limited (403); trying next key...")
                time.sleep(backoff_seconds)
                continue
            elif i < len(rotated_keys) - 1:
                print(f"⚠️ API key #{(key_rotation_index + i)%len(api_keys)+1} failed ({e}); trying next key...")
                time.sleep(backoff_seconds)
                continue
            else:
                print(f"⚠️ Failed to fetch job detail for {job_id} (all API keys exhausted): {e}")
                return None
        except Exception as e:
            if i < len(rotated_keys) - 1:
                print(f"⚠️ API key #{(key_rotation_index + i)%len(api_keys)+1} error ({e}); trying next key...")
                time.sleep(backoff_seconds)
                continue
            else:
                print(f"⚠️ Failed to fetch job detail for {job_id}: {e}")
                return None
    
    return None


def parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        # Try ISO-8601 format first
        d = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    
    try:
        # Try Reed's DD/MM/YYYY format
        d = datetime.strptime(dt, "%d/%m/%Y")
        return d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_salary_from_text(description: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Extract salary min/max from free text when API fields are missing.

    Returns (min_salary, max_salary, currency_guess) where currency is 'GBP'
    when a pound sign/GBP hint is present, else None.
    """
    if not description:
        return None, None, None

    # Decode HTML entities (e.g., &#163; -> £)
    import html
    description = html.unescape(description)
    
    text = description.lower()
    currency = 'GBP' if '£' in description or 'gbp' in text or 'per annum' in text else None

    def _to_number(token: str, has_k: bool) -> Optional[float]:
        try:
            clean = token.replace(',', '')
            num = float(clean)
            return num * 1000 if has_k else num
        except Exception:
            return None

    # Patterns to capture ranges - prioritize salary context
    range_patterns = [
        # Explicit salary markers with ranges
        r"(?:salary|compensation|package|pay|remuneration)[\s:]*(?:circa|approx\.?|approximately)?\s*£?\s?(?P<min>\d{1,3}(?:[, ]\d{3})+|\d+)(?P<min_k>k)?\s*(?:to|-|–|—)\s*£?\s?(?P<max>\d{1,3}(?:[, ]\d{3})+|\d+)(?P<max_k>k)?",
        # Standard range with currency symbol
        r"£\s?(?P<min>\d{1,3}(?:[, ]\d{3})+|\d+)(?P<min_k>k)?\s*(?:to|-|–|—)\s*£?\s?(?P<max>\d{1,3}(?:[, ]\d{3})+|\d+)(?P<max_k>k)?",
        # Between X and Y format
        r"between\s+£?\s?(?P<min>\d{1,3}(?:[, ]\d{3})+|\d+)(?P<min_k>k)?\s+and\s+£?\s?(?P<max>\d{1,3}(?:[, ]\d{3})+|\d+)(?P<max_k>k)?",
        # From X to Y format
        r"from\s+£?\s?(?P<min>\d{1,3}(?:[, ]\d{3})+|\d+)(?P<min_k>k)?\s+(?:to|up to)\s+£?\s?(?P<max>\d{1,3}(?:[, ]\d{3})+|\d+)(?P<max_k>k)?",
    ]

    for pat in range_patterns:
        m = re.search(pat, description, re.IGNORECASE)
        if m:
            min_val = _to_number(m.group('min'), bool(m.group('min_k')))
            max_val = _to_number(m.group('max'), bool(m.group('max_k')))
            if min_val is not None and max_val is not None:
                lo, hi = sorted((min_val, max_val))
                return lo, hi, currency

    # Single-sided patterns: "up to 50k", "from 45k"
    up_to = re.search(r"up to\s+£?\s?(?P<max>\d{1,3}(?:[ ,]\d{3})?|\d+(?:\.\d+)?)(?P<max_k>k)?", description, re.IGNORECASE)
    from_only = re.search(r"from\s+£?\s?(?P<min>\d{1,3}(?:[ ,]\d{3})?|\d+(?:\.\d+)?)(?P<min_k>k)?", description, re.IGNORECASE)
    if up_to:
        max_val = _to_number(up_to.group('max'), bool(up_to.group('max_k')))
        if max_val is not None:
            return max_val, max_val, currency
    if from_only:
        min_val = _to_number(from_only.group('min'), bool(from_only.group('min_k')))
        if min_val is not None:
            return min_val, min_val, currency

    # Try to extract salary from job title (e.g., "Lead Engineer - £75,000")
    title_pattern = re.search(r"[-/]\s*£\s?(\d{1,3}(?:[, ]\d{3})*|\d+)(k)?", description[:200], re.IGNORECASE)
    if title_pattern:
        val = _to_number(title_pattern.group(1), bool(title_pattern.group(2)))
        if val is not None and (val >= 10000 or (val >= 10 and title_pattern.group(2))):
            return val, val, currency

    # Fallback: grab salary-like numbers (filter out small values like "2-3 days")
    # Exclude common non-salary contexts: "X days", "X months", "X years"
    exclude_pattern = re.compile(r"\d+\s*(?:day|days|month|months|year|years|week|weeks|x|times|%|percent)", re.IGNORECASE)
    
    number_pattern = re.compile(r"£\s?(\d{1,3}(?:[, ]\d{3})+|\d+)(k)?")
    numbers = []
    for match in number_pattern.finditer(description):
        n, kflag = match.groups()
        val = _to_number(n, bool(kflag))
        
        # Check if this number appears in a non-salary context
        start, end = match.span()
        context = description[max(0, start-20):min(len(description), end+30)]
        
        # Skip if this number is part of "X days/months/years" etc
        if exclude_pattern.search(context):
            continue
            
        if val is not None and (val >= 10000 or (val >= 10 and kflag)):
            numbers.append(val)

    if not numbers:
        return None, None, currency
    if len(numbers) == 1:
        return numbers[0], numbers[0], currency

    lo, hi = sorted(numbers[:2])
    return lo, hi, currency


def detect_salary_type(title: str, description: str, salary_min: Optional[float] = None, salary_max: Optional[float] = None) -> Optional[str]:
    """Infer salary type from title/description text with improved prioritization.

    Returns one of: 'per annum', 'per month', 'per week', 'per day', 'per hour', or None.
    
    Detection strategy:
    1. Look for rate indicators near salary amounts (most reliable)
    2. Only if no rate found near amount, check for explicit per annum indicators
    3. Use salary range heuristics as final fallback
    
    Args:
        title: Job title
        description: Job description
        salary_min: Optional minimum salary (for better heuristic)
        salary_max: Optional maximum salary (for better heuristic)
    """
    if not (title or description):
        return None

    # If both salary bounds are explicitly zero, treat as unknown and avoid assigning a type
    if salary_min == 0 and salary_max == 0:
        return None
    
    text = f"{title or ''} {description or ''}".lower()
    
    # FIRST: Check for rate indicators near salary amounts
    # These patterns look for rate words near actual numbers (including comma-separated)
    patterns = [
        # Per hour - specific patterns only
        (r'\b(?:£|p)?\d+(?:,\d{3})*\.?\d*\s*(?:per\s*hour|ph\b|p/h|/hour|hourly|\bhour\s*rate)', 'per hour'),
        (r'\b(?:per\s*hour|ph\b|p/h|/hour|hourly|\bhour\s*rate)\s*(?:£|p)?\d+(?:,\d{3})*', 'per hour'),
        # Per day - specific patterns only
        (r'\b(?:£|p)?\d+(?:,\d{3})*\.?\d*\s*(?:per\s*day|pd\b|p/d|/day|day\s*rate|daily)', 'per day'),
        (r'\b(?:per\s*day|pd\b|p/d|/day|day\s*rate|daily)\s*(?:£|p)?\d+(?:,\d{3})*', 'per day'),
        # Per week - specific patterns only
        (r'\b(?:£|p)?\d+(?:,\d{3})*\.?\d*\s*(?:per\s*week|pw\b|p/w|/week)', 'per week'),
        (r'\b(?:per\s*week|pw\b|p/w|/week)\s*(?:£|p)?\d+(?:,\d{3})*', 'per week'),
        # Per month - specific patterns only
        (r'\b(?:£|p)?\d+(?:,\d{3})*\.?\d*\s*(?:per\s*month|pcm|p\.c\.m|/month|monthly)', 'per month'),
        (r'\b(?:per\s*month|pcm|p\.c\.m|/month|monthly)\s*(?:£|p)?\d+(?:,\d{3})*', 'per month'),
    ]
    
    for pattern, sal_type in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Extract the number to sanity-check against type
            # Look for £XXX,XXX or £XXX format numbers in the match
            numbers_in_match = re.findall(r'£?\s?(\d+(?:,\d{3})*(?:\.\d+)?)', match.group())
            if numbers_in_match:
                try:
                    val = float(numbers_in_match[0].replace(',', ''))
                    # Sanity checks: avoid unrealistic combinations
                    if sal_type in ('per hour', 'per day') and val >= 5000:
                        # £5000+ per hour/day is extremely unrealistic, likely annual
                        continue
                    return sal_type
                except (ValueError, IndexError):
                    return sal_type
            else:
                return sal_type
    
    # SECOND: Check for explicit per annum indicators (only if no rate found near amount)
    pa_indicators = [
        r'\bper annum\b',
        r'\bp\.a\.?\b',
        r'\bper year\b',
        r'\bper annual\b',
        r'annually'
    ]
    
    if any(re.search(pat, text, re.IGNORECASE) for pat in pa_indicators):
        return 'per annum'
    
    # THIRD: Use actual parsed salary values for heuristic (most reliable fallback)
    # If we have the parsed salary amounts, use them
    if salary_min is not None and salary_min > 0:
        # Single value or range - if >= 5001, it's almost certainly annual
        # unless explicitly marked otherwise above
        if salary_min >= 5001 or (salary_max and salary_max >= 5001):
            return 'per annum'
        
        # For lower values, use salary range heuristics
        if salary_min <= 50:
            return 'per hour'
        elif salary_min <= 200:
            return 'per day'
        elif salary_min <= 5000:
            # Ambiguous - could be daily or monthly, default to daily
            return 'per day'
    
    # FOURTH: Fallback to text-based extraction (least reliable)
    if re.search(r'\b(?:£|p)?\d+', text):
        numbers = re.findall(r'(?:£|p)?(\d+(?:,\d{3})*(?:\.\d+)?)', text)
        if numbers:
            try:
                first_num = float(numbers[0].replace(',', ''))
                
                # Heuristic ranges based on typical UK salary patterns
                if first_num <= 50:  # £12-50 almost always hourly
                    return 'per hour'
                elif 51 <= first_num <= 200:  # £51-200 usually daily
                    return 'per day'
                elif 201 <= first_num <= 5000:  # £201-5000 usually daily
                    return 'per day'
                elif first_num >= 5001:  # £5000+ almost always annual
                    return 'per annum'
            except (ValueError, IndexError):
                pass
    
    return None


def standardize_location(location_name: str) -> str:
    """
    Standardize location names by converting UK postcodes to city names.
    
    Maintains city names as-is, converts postcodes to their district/city names.
    Handles common UK postcode prefixes and remote variations.
    
    Args:
        location_name: Raw location string (can be city name or postcode)
        
    Returns:
        Standardized city/district name
        
    Examples:
        'B111AD' → 'Birmingham'
        'SW1A1AA' → 'London'
        'London' → 'London'
        'Remote' → 'Remote/Flexible'
    """
    if not location_name:
        return location_name
    
    loc_clean = location_name.strip()
    loc_upper = loc_clean.upper()
    
    # Handle remote/flexible work
    if any(term in loc_upper for term in ['REMOTE', 'WORK FROM HOME', 'WFH', 'HYBRID', 'FLEXIBLE']):
        return 'Remote/Flexible'
    
    # Mapping of UK postcode prefixes to city names
    postcode_map = {
        'AL': 'Aldershot', 'B1': 'Birmingham', 'B2': 'Birmingham', 'B3': 'Birmingham', 'B4': 'Birmingham',
        'B5': 'Birmingham', 'B6': 'Birmingham', 'B7': 'Birmingham', 'B8': 'Birmingham',
        'B9': 'Birmingham', 'B10': 'Birmingham', 'B11': 'Birmingham', 'B12': 'Birmingham',
        'B13': 'Birmingham', 'B14': 'Birmingham', 'B15': 'Birmingham', 'B16': 'Birmingham',
        'B17': 'Birmingham', 'B18': 'Birmingham', 'B19': 'Birmingham', 'B20': 'Birmingham',
        'B21': 'Birmingham', 'B22': 'Birmingham', 'B23': 'Birmingham', 'B24': 'Birmingham',
        'B25': 'Birmingham', 'B26': 'Birmingham', 'B27': 'Birmingham', 'B28': 'Birmingham',
        'B29': 'Birmingham', 'B30': 'Birmingham', 'B31': 'Birmingham', 'B32': 'Birmingham',
        'B33': 'Birmingham', 'B34': 'Birmingham', 'B35': 'Birmingham', 'B36': 'Birmingham',
        'B37': 'Birmingham', 'B38': 'Birmingham', 'B39': 'Birmingham', 'B40': 'Birmingham',
        'B41': 'Birmingham', 'B42': 'Birmingham', 'B43': 'Birmingham', 'B44': 'Birmingham',
        'B45': 'Birmingham', 'B46': 'Birmingham', 'B47': 'Birmingham', 'B48': 'Birmingham',
        'B49': 'Birmingham', 'BA': 'Bath', 'BB': 'Blackburn', 'BD': 'Bradford',
        'BH': 'Bournemouth', 'BL': 'Bolton', 'BN': 'Brighton', 'BR': 'Bromley',
        'BS': 'Bristol', 'BT': 'Belfast', 'CA': 'Carlisle', 'CB': 'Cambridge',
        'CF': 'Cardiff', 'CH': 'Chester', 'CM': 'Chelmsford', 'CO': 'Colchester',
        'CR': 'Croydon', 'CT': 'Canterbury', 'CV': 'Coventry', 'CW': 'Crewe',
        'DA': 'Dartford', 'DD': 'Dundee', 'DE': 'Derby', 'DG': 'Dumfries',
        'DH': 'Sunderland', 'DL': 'Darlington', 'DN': 'Doncaster', 'DT': 'Dorchester',
        'DY': 'Dudley', 'E': 'London', 'EC': 'London', 'ED': 'Edinburgh',
        'EH': 'Edinburgh', 'EN': 'Enfield', 'EX': 'Exeter', 'FK': 'Falkirk',
        'FY': 'Fylde', 'G': 'Glasgow', 'GL': 'Gloucester', 'GU': 'Guildford',
        'HA': 'Harrow', 'HD': 'Huddersfield', 'HG': 'Harrogate', 'HP': 'Hemel Hempstead',
        'HR': 'Hereford', 'HS': 'Hebrides', 'HU': 'Hull', 'HX': 'Halifax',
        'IG': 'Ilford', 'IP': 'Ipswich', 'IV': 'Inverness', 'JE': 'Jersey',
        'KA': 'Kilmarnock', 'KT': 'Kingston', 'KY': 'Kirkcaldy', 'L': 'Liverpool',
        'LA': 'Lancaster', 'LD': 'Llandrindod Wells', 'LE': 'Leicester', 'LN': 'Lincoln',
        'LS': 'Leeds', 'LU': 'Luton', 'M': 'Manchester', 'ME': 'Medway',
        'MK': 'Milton Keynes', 'ML': 'Motherwell', 'N': 'London', 'NE': 'Newcastle',
        'NG': 'Nottingham', 'NN': 'Northampton', 'NP': 'Newport', 'NR': 'Norwich',
        'NW': 'London', 'OL': 'Oldham', 'OX': 'Oxford', 'PA': 'Paisley',
        'PE': 'Peterborough', 'PH': 'Perth', 'PL': 'Plymouth', 'PN': 'Penrith',
        'PO': 'Portsmouth', 'PR': 'Preston', 'RG': 'Reading', 'RH': 'Redhill',
        'RM': 'Romford', 'S': 'Sheffield', 'SA': 'Swansea', 'SE': 'London',
        'SG': 'Stevenage', 'SK': 'Stockport', 'SL': 'Slough', 'SM': 'Sutton',
        'SN': 'Swindon', 'SO': 'Southampton', 'SP': 'Salisbury', 'SR': 'Sunderland',
        'SS': 'Southend', 'ST': 'Stoke-on-Trent', 'SW': 'London', 'SY': 'Shrewsbury',
        'TA': 'Taunton', 'TD': 'Galashiels', 'TF': 'Telford', 'TN': 'Tonbridge',
        'TR': 'Truro', 'TS': 'Middlesbrough', 'TW': 'Twickenham', 'TY': 'Tywyn',
        'UB': 'Uxbridge', 'UL': 'Ullapool', 'W': 'London', 'WA': 'Warrington',
        'WC': 'London', 'WD': 'Watford', 'WF': 'Wakefield', 'WN': 'Wigan',
        'WR': 'Worcester', 'WS': 'Walsall', 'WV': 'Wolverhampton', 'YO': 'York', 'ZE': 'Shetland',
    }
    
    # Detect if this looks like a postcode vs a city name
    # Postcodes have: letter-letter pattern, contain digits, and have 6-7 chars
    # City names typically have: no digits, or are already formatted as city names
    is_likely_postcode = (
        len(loc_upper) >= 5 and 
        len(loc_upper) <= 8 and
        any(c.isdigit() for c in loc_upper) and
        any(c.isalpha() for c in loc_upper)
    )
    
    # Only try postcode mapping if it looks like a postcode
    if is_likely_postcode:
        # Try to match against postcode map (longer prefixes first)
        for prefix in sorted(postcode_map.keys(), key=len, reverse=True):
            if loc_upper.startswith(prefix):
                return postcode_map[prefix]
    
    # Return as-is (preserve original formatting for city names)
    return loc_clean


def detect_employment_terms(title: str, description: str) -> Tuple[Optional[bool], Optional[bool], Optional[str]]:
    """Infer full_time, part_time, and contract_type from title/description text.

    contract_type one of: 'permanent', 'fixed term', 'contract', 'temporary', else None.
    """
    text = f"{title or ''} {description or ''}".lower()

    full_terms = ['full time', 'full-time', 'fulltime', 'ft role']
    part_terms = ['part time', 'part-time', 'parttime', 'pt role']

    perm_terms = ['permanent', 'perm role', 'perm position', 'perm basis']
    fixed_terms = ['fixed term', 'ftc', 'fixed-term']
    contract_terms = ['contract role', 'contract position', 'contract basis', 'on contract']
    temp_terms = ['temporary', 'temp role', 'temp position', 'interim']

    full_time = True if any(tok in text for tok in full_terms) else None
    part_time = True if any(tok in text for tok in part_terms) else None

    contract_type = None
    if any(tok in text for tok in perm_terms):
        contract_type = 'permanent'
    elif any(tok in text for tok in fixed_terms):
        contract_type = 'fixed term'
    elif any(tok in text for tok in contract_terms):
        contract_type = 'contract'
    elif any(tok in text for tok in temp_terms):
        contract_type = 'temporary'

    # If we know it is permanent and nothing suggests part-time, assume full-time
    if full_time is None and contract_type == 'permanent' and part_time is not True:
        full_time = True

    return full_time, part_time, contract_type

def extract_skills(description: str, skill_patterns: set = None, skill_aliases: dict = None) -> List[str]:
    """
    Extract skills from job description text using word-boundary regex matching.
    
    Normalizes extracted skills via aliases and filters to exact matches only
    (prevents "r" matching "REST", "ai" matching "TRAIN", etc.)
    
    Args:
        description: Job description text to search
        skill_patterns: Set of skill keywords to match (defaults to SKILL_PATTERNS)
        skill_aliases: Dict mapping skill variations to canonical names
        
    Returns:
        List of canonical skill names found in description (lowercase, deduplicated)
        
    Example:
        >>> extract_skills("Python and SQL expertise required")
        ['python', 'sql']
    """
    """
    Extract technical skills from job description text.
    Uses word-boundary regex pattern matching to reduce false positives.
    
    Args:
        description: Job description text to search
        skill_patterns: Set of skills to search for (uses default if None)
        skill_aliases: Dict mapping variations to canonical names (uses default if None)
    
    Returns:
        List of canonical skill names found in the description
    """
    if not description:
        return []
    
    if skill_patterns is None:
        skill_patterns = SKILL_PATTERNS
    
    if skill_aliases is None:
        skill_aliases = SKILL_ALIASES
    
    description_lower = description.lower()
    found_skills = set()
    
    # Whitelist of allowed short skills (2-3 chars) that are legitimate
    allowed_short_skills = {'sql', 'api', 'aws', 'git', 'etl', 'nlp', 'gcp', 'css', 'sas', 'dax', 'vba'}
    
    # Check for each skill pattern with word boundaries
    for skill in skill_patterns:
        # Skip very short tokens (< 3 chars) unless they're in the whitelist
        if len(skill) < 3 and skill.lower() not in allowed_short_skills:
            continue
        # Use word boundary regex for better precision
        # Escape special regex characters in skill pattern
        escaped_skill = re.escape(skill)
        # Match whole words/phrases (word boundaries or punctuation)
        pattern = r'\b' + escaped_skill + r'\b'
        if re.search(pattern, description_lower, re.IGNORECASE):
            # Normalize to canonical name if it's an alias
            canonical_skill = skill_aliases.get(skill, skill)
            found_skills.add(canonical_skill)
    
    # Return sorted list
    return sorted(list(found_skills))


def extract_skills_with_context(description: str, skill_patterns: set = None, skill_aliases: dict = None) -> List[Tuple[str, str, str]]:
    """
    Extract skills with contextual metadata: importance level and proficiency level.
    
    Searches for patterns indicating skill importance:
      - Essential: "essential", "required", "must have", "mandatory"
      - Preferred: "preferred", "nice to have", "beneficial", "valuable"
      - Default: "nice_to_have" if no indicator found
    
    Searches for patterns indicating proficiency:
      - Expert: "expert", "expertise", "advanced"
      - Senior: "senior", "experienced", "proficient"
      - Intermediate: "working knowledge", "familiar", "good"
      - Basic: "basic", "fundamentals", "introduction"
      - Default: None if no indicator found
    
    Args:
        description: Job description text
        skill_patterns: Set of skill keywords to extract
        skill_aliases: Dict for skill name normalization
        
    Returns:
        List of tuples: (skill_name, importance_level, proficiency_level)
        
    Example:
        >>> extract_skills_with_context("Essential knowledge of Python")
        [('python', 'essential', None)]
    """
    """
    Extract skills with importance level and proficiency indicators.
    
    Args:
        description: Job description text to search
        skill_patterns: Set of skills to search for (uses default if None)
        skill_aliases: Dict mapping variations to canonical names (uses default if None)
    
    Returns:
        List of tuples: (canonical_skill, importance_level, proficiency_level)
        - importance_level: 'essential', 'preferred', 'nice_to_have', 'unknown'
        - proficiency_level: 'expert', 'senior', 'intermediate', 'basic', 'unknown'
    """
    if not description:
        return []
    
    if skill_patterns is None:
        skill_patterns = SKILL_PATTERNS
    
    if skill_aliases is None:
        skill_aliases = SKILL_ALIASES
    
    description_lower = description.lower()
    found_skills = []
    
    # Whitelist of allowed short skills (2-3 chars) that are legitimate
    allowed_short_skills = {'sql', 'api', 'aws', 'git', 'etl', 'nlp', 'gcp', 'css', 'sas', 'dax', 'vba'}
    
    # Importance indicators (ordered by priority for overlapping matches)
    importance_patterns = {
        'essential': [
            r'essential\s+(?:knowledge|experience|skills?)[^.]{0,100}?{skill}',
            r'{skill}\s+(?:is |are )?(?:essential|required|mandatory|must have)',
            r'(?:must|required to) (?:have|know|understand)[^.]{0,100}?{skill}',
            r'strong (?:knowledge|experience|understanding|proficiency)[^.]{0,100}?{skill}',
            r'proven (?:knowledge|experience|understanding)[^.]{0,100}?{skill}',
        ],
        'preferred': [
            r'prefer(?:red|ably|ence)[^.]{0,100}?{skill}',
            r'{skill}\s+(?:is |are )?(?:preferred|desirable)',
            r'ideally[^.]{0,100}?{skill}',
        ],
        'nice_to_have': [
            r'nice to have[^.]{0,100}?{skill}',
            r'{skill}\s+(?:would be )?(?:a plus|beneficial|advantageous)',
            r'bonus[^.]{0,100}?{skill}',
        ]
    }
    
    # Proficiency indicators
    proficiency_patterns = {
        'expert': [
            r'expert(?:ise)? (?:in |with |using )?{skill}',
            r'{skill}\s+expert',
            r'deep (?:knowledge|understanding|experience) (?:of |in |with )?{skill}',
            r'mastery (?:of |in )?{skill}',
        ],
        'senior': [
            r'senior[^.]{0,50}?{skill}',
            r'advanced[^.]{0,50}?{skill}',
            r'{skill}\s+(?:at an? )?advanced (?:level)?',
            r'extensive (?:knowledge|experience) (?:of |in |with )?{skill}',
        ],
        'intermediate': [
            r'intermediate[^.]{0,50}?{skill}',
            r'working knowledge (?:of |in )?{skill}',
            r'proficien(?:t|cy) (?:in |with )?{skill}',
            r'solid (?:knowledge|understanding|experience) (?:of |in |with )?{skill}',
            r'strong (?:skills? |knowledge |understanding )?(?:in |with |of )?{skill}',
            r'experience (?:with |in |using )?{skill}',
            r'knowledge of {skill}',
        ],
        'basic': [
            r'basic[^.]{0,50}?{skill}',
            r'fundamental[^.]{0,50}?{skill}',
            r'familiarit(?:y|ies) (?:with |in )?{skill}',
            r'exposure to {skill}',
            r'understanding of {skill}',
        ]
    }
    
    # Check for each skill pattern
    for skill in skill_patterns:
        # Skip very short tokens unless whitelisted
        if len(skill) < 3 and skill.lower() not in allowed_short_skills:
            continue
        
        escaped_skill = re.escape(skill)
        skill_pattern = r'\b' + escaped_skill + r'\b'
        
        if re.search(skill_pattern, description_lower, re.IGNORECASE):
            canonical_skill = skill_aliases.get(skill, skill)
            
            # Detect importance level
            importance = 'unknown'
            for level, patterns in importance_patterns.items():
                for pattern_template in patterns:
                    pattern = pattern_template.replace('{skill}', escaped_skill)
                    if re.search(pattern, description_lower, re.IGNORECASE):
                        importance = level
                        break
                if importance != 'unknown':
                    break
            
            # Detect proficiency level
            proficiency = 'unknown'
            for level, patterns in proficiency_patterns.items():
                for pattern_template in patterns:
                    pattern = pattern_template.replace('{skill}', escaped_skill)
                    if re.search(pattern, description_lower, re.IGNORECASE):
                        proficiency = level
                        break
                if proficiency != 'unknown':
                    break
            
            found_skills.append((canonical_skill, importance, proficiency))
    
    return found_skills


def detect_work_location_type(title: str, description: str) -> str:
    """
    Classify job as remote, hybrid, office, or unknown based on title and description.
    
    Detection priority (first match wins):
      1. Remote: "remote", "work from home", "wfh", "anywhere"
      2. Hybrid: "hybrid", "flexible", "3 days", "2 days", "onsite & remote"
      3. Office: "onsite", "office", "in-person", location + "based" (London-based, NYC-based, etc.)
      4. Unknown: Default if no patterns match
    
    Args:
        title: Job title string
        description: Job description text
        
    Returns:
        One of: "remote", "hybrid", "office", "unknown"
    """
    """
    Detect work location type from job title and description.
    
    Args:
        title: Job title text
        description: Job description text
    
    Returns:
        One of: 'remote', 'hybrid', 'office', 'unknown'
    """
    text = f"{title or ''} {description or ''}".lower()
    
    # Remote indicators (strong signals) - require context to avoid "remote access tools" false positives
    remote_patterns = [
        r'\b(?:fully |100% |completely )?remote\s+(?:role|position|job|location|working|working)\b',
        r'\bwork from home\b',
        r'\bwfh\b',
        r'\b(?:fully |100% )?home.?based\b',
        r'\bremote.?(?:first|only)\b',
        r'\banywhere in (?:the )?uk\b',
        r'\b100% remote\b',
    ]
    
    # Hybrid indicators
    hybrid_patterns = [
        r'\bhybrid\b',
        r'\b\d+\s*days?\s+(?:in|at|in the|per)\s+(?:the )?office\b',
        r'\b\d+\s*days?\s+(?:remote|home|wfh)\b',
        r'\bflexible\s+(?:working|work)\b',
        r'\bpart.?remote\b',
        r'\bcombination of (?:remote|home) and office\b',
        r'\b(?:remotely or on.?site|on.?site or remotely)\b',
        r'\boffice and remote\b',
        r'\bremote and office\b',
    ]
    
    # Office/On-site indicators (only if no remote/hybrid)
    office_patterns = [
        r'\bfully office.?based\b',
        r'\b(?:100% |fully |completely )?on.?site\b',
        r'\bin.?office\b',
        r'\boffice.?based\b',
        r'\bat our office\b',
        r'\bbased\s+(?:in|at)\s+(?:[a-z\s]+?)\b',  # "based in London", "based at Manchester"
    ]
    
    # Check in priority order: remote > hybrid > office
    for pattern in remote_patterns:
        if re.search(pattern, text):
            # Check if it's actually hybrid
            for hybrid_pattern in hybrid_patterns:
                if re.search(hybrid_pattern, text):
                    return 'hybrid'
            return 'remote'
    
    for pattern in hybrid_patterns:
        if re.search(pattern, text):
            return 'hybrid'
    
    for pattern in office_patterns:
        if re.search(pattern, text):
            # Double-check it's not hybrid or remote
            for hybrid_pattern in hybrid_patterns:
                if re.search(hybrid_pattern, text):
                    return 'hybrid'
            for remote_pattern in remote_patterns:
                if re.search(remote_pattern, text):
                    return 'remote'
            return 'office'
    
    return 'unknown'


def detect_seniority_level(title: str, description: str) -> str:
    """
    Classify job seniority level based on title and description keywords.
    
    Detection priority (first match wins):
      1. Executive: "ceo", "cto", "cfo", "chief"
      2. Director: "director", "head of"
      3. Manager: "manager", "team lead", "team manager"
      4. Lead: "senior", "lead", "principal", "staff"
      5. Senior: "senior", "sr.", "sr ", "veteran"
      6. Mid-level: "intermediate", "mid-level" (default fallback)
      7. Junior: "junior", "jr.", "jr ", "graduate"
      8. Entry: "entry level", "entry-level", "trainee"
      
    Falls back to 'mid' if no patterns match (most common scenario).
    
    Args:
        title: Job title string
        description: Job description text
        
    Returns:
        One of: "executive", "director", "manager", "lead", "senior", "mid", "junior", "entry"
    """
    """
    Detect seniority level from job title and description.
    
    Args:
        title: Job title text
        description: Job description text
    
    Returns:
        One of: 'executive', 'director', 'manager', 'lead', 'senior', 'mid', 'junior', 'entry', 'unknown'
    """
    text = f"{title or ''} {description or ''}".lower()
    title_lower = (title or '').lower()
    
    # Priority order: executive > director > manager > lead > senior > mid > junior > entry
    
    # Executive level (C-suite)
    executive_patterns = [
        r'\b(?:chief|c-level|cto|cio|cdo|ceo|cfo)\b',
        r'\bvp\b',
        r'\bvice president\b',
    ]
    
    # Director level
    director_patterns = [
        r'\bdirector\b',
        r'\bhead of\b',
    ]
    
    # Manager level
    manager_patterns = [
        r'\bmanager\b',
        r'\bmgr\b',
        r'\bmanagement\b',
    ]
    
    # Lead/Principal level
    lead_patterns = [
        r'\b(?:lead|principal|staff)\b',
        r'\btech lead\b',
        r'\bteam lead\b',
    ]
    
    # Senior level
    senior_patterns = [
        r'\bsenior\b',
        r'\bsr\.?\b',
        r'\bsnr\b',
    ]
    
    # Junior level
    junior_patterns = [
        r'\bjunior\b',
        r'\bjr\.?\b',
        r'\bgraduate\b',
        r'\bgrad\b',
        r'\bassociate\b',
    ]
    
    # Entry level
    entry_patterns = [
        r'\bentry.?level\b',
        r'\btrainee\b',
        r'\bintern\b',
        r'\bapprentice\b',
    ]
    
    # Check title first (higher priority), then full text
    # Executive
    for pattern in executive_patterns:
        if re.search(pattern, title_lower):
            return 'executive'
    
    # Director
    for pattern in director_patterns:
        if re.search(pattern, title_lower):
            return 'director'
    
    # Manager
    for pattern in manager_patterns:
        if re.search(pattern, title_lower):
            return 'manager'
    
    # Lead
    for pattern in lead_patterns:
        if re.search(pattern, title_lower):
            return 'lead'
    
    # Senior
    for pattern in senior_patterns:
        if re.search(pattern, title_lower):
            return 'senior'
    
    # Junior
    for pattern in junior_patterns:
        if re.search(pattern, title_lower):
            return 'junior'
    
    # Entry
    for pattern in entry_patterns:
        if re.search(pattern, title_lower):
            return 'entry'
    
    # If no clear indicator in title, check description for years of experience
    # Look for experience requirements
    exp_match = re.search(r'(\d+)\+?\s*(?:years?|yrs?)\s+(?:of\s+)?experience', text)
    if exp_match:
        years = int(exp_match.group(1))
        if years >= 10:
            return 'senior'
        elif years >= 5:
            return 'mid'
        elif years >= 2:
            return 'mid'
        else:
            return 'junior'
    
    # Default to mid-level if no clear indicators
    return 'mid'


def detect_job_role_category(title: str, job_role_categories: Dict[str, set] = None) -> Optional[str]:
    """
    Classify job into a role category based on job title keywords.
    
    Detection:
      - Checks job title for keywords matching predefined role categories
      - Returns the first matching category (priority order follows config order)
      - Falls back to 'Other' if no match
    
    Args:
        title: Job title string
        job_role_categories: Dict mapping category names to keyword sets
        
    Returns:
        One of the category names from job_role_categories, or 'Other' if no match
        
    Example:
        >>> categories = {'Engineering': {'engineer', 'developer'}, 'Analyst': {'analyst'}}
        >>> detect_job_role_category('Senior Data Engineer', categories)
        'Engineering'
        >>> detect_job_role_category('Product Manager', categories)
        'Other'
    """
    if not title or not job_role_categories:
        return 'Other'
    
    title_lower = title.lower()
    
    # Check each category in order
    for category_name, keywords in job_role_categories.items():
        for keyword in keywords:
            # Use word boundary matching to prevent partial matches
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, title_lower):
                return category_name
    
    return 'Other'


def stable_hash(obj: Any) -> str:
    """Hash canonical JSON to detect content changes"""
    enc = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(enc).hexdigest()


def shape_row(
    cfg: Dict[str, Any],
    job: Dict[str, Any]
) -> Tuple:
    """
    Transform API job object into normalized database row tuple.
    
    Returns tuple structure for landing.raw_jobs insertion:
      (source_name, job_id, title, employer, location, posted_at, expires_at, 
       date_modified, raw_json, content_hash)
    
    The content_hash enables efficient change detection - only jobs with modified
    content get re-transformed to staging, avoiding redundant processing.
    
    Args:
        cfg: Configuration dict (SOURCE_NAME, JOB_ID_KEY, etc.)
        job: Raw job object from Reed API
        
    Returns:
        10-element tuple ready for landing table INSERT
    """
    # Flexible field access
    job_id = str(job.get(cfg["JOB_ID_KEY"]))
    title = job.get("jobTitle") or job.get("title")
    employer = job.get("employerName") or job.get("employer") or job.get("company")
    location = job.get("locationName") or job.get("location")
    posted_at = parse_iso(job.get(cfg["POSTED_DATE_KEY"]))
    expires_at = parse_iso(job.get(cfg["EXPIRY_DATE_KEY"]))
    date_modified = parse_iso(job.get(cfg["DATE_MODIFIED_KEY"]))
    chash = stable_hash(job)

    return (
        cfg["SOURCE_NAME"],
        job_id,
        title,
        employer,
        location,
        posted_at,
        expires_at,
        date_modified,
        psycopg2.extras.Json(job),
        chash,
    )


# ---------- Cleanup and monitoring functions ----------

def cleanup_expired_jobs(conn, cfg: Dict[str, Any]) -> int:
    """
    Remove expired job listings from staging.jobs_v1.
    
    Deletes all jobs where expires_at < NOW(). Can be disabled via
    ENABLE_EXPIRATION_CLEANUP config flag.
    
    Args:
        conn: PostgreSQL database connection
        cfg: Configuration dict with ENABLE_EXPIRATION_CLEANUP boolean
        
    Returns:
        Number of jobs deleted (0 if feature disabled or no expired jobs)
        
    Side Effects:
        Modifies staging.jobs_v1 table, commits transaction
    """
    if not cfg.get("ENABLE_EXPIRATION_CLEANUP", True):
        return 0
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM staging.jobs_v1
                WHERE expires_at IS NOT NULL AND expires_at < NOW()
            """)
            deleted = cur.rowcount
            if deleted > 0:
                print(f"🧹 Cleanup: Removed {deleted} expired job listings")
            conn.commit()
            return deleted
    except Exception as e:
        print(f"⚠️ Cleanup warning: Failed to remove expired jobs: {e}")
        return 0


def log_enrichment_stats(conn) -> None:
    """
    Log enrichment statistics about job descriptions.
    
    Reports:
      - Total jobs in staging.jobs_v1
      - Number with full descriptions (length > 450 chars)
      - Number missing descriptions
      - Enrichment rate percentage
      
    Used to monitor quality of description fetching from detail endpoint.
    Enables target of 100% enrichment rate (all jobs with full descriptions).
    
    Args:
        conn: PostgreSQL database connection
        
    Side Effects:
        Prints formatted statistics to stdout (Azure Functions logs)
    """
    try:
        with conn.cursor() as cur:
            # Count jobs with full descriptions (not truncated at 453 chars)
            cur.execute("""
                SELECT 
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN LENGTH(COALESCE(job_description, '')) > 450 THEN 1 ELSE 0 END) as enriched_jobs,
                    SUM(CASE WHEN LENGTH(COALESCE(job_description, '')) = 0 THEN 1 ELSE 0 END) as missing_descriptions
                FROM staging.jobs_v1
                WHERE source_name = 'reed'
            """)
            row = cur.fetchone()
            if row:
                total, enriched, missing = row
                print(f"📊 Enrichment stats: Total jobs={total}, Enriched={enriched}, Missing={missing}")
                if total > 0:
                    enrichment_pct = (enriched / total) * 100
                    print(f"   Enrichment rate: {enrichment_pct:.1f}%")
    except Exception as e:
        print(f"⚠️ Monitoring warning: Failed to log enrichment stats: {e}")


def log_skill_extraction_stats(conn) -> None:
    """Log statistics about skill extraction"""
    try:
        with conn.cursor() as cur:
            # Count unique skills and jobs with skills
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT skill) as unique_skills,
                    COUNT(DISTINCT job_id) as jobs_with_skills,
                    COUNT(*) as total_skill_matches
                FROM staging.job_skills
                WHERE source_name = 'reed'
            """)
            row = cur.fetchone()
            if row:
                unique_skills, jobs_with_skills, total_matches = row
                print(f"🎯 Skill extraction: Unique skills={unique_skills}, Jobs with skills={jobs_with_skills}, Total matches={total_matches}")
    except Exception as e:
        print(f"⚠️ Monitoring warning: Failed to log skill stats: {e}")


def detect_and_report_duplicates(conn) -> None:
    """
    Comprehensive duplicate detection across all data layers
    Checks landing, staging, and junction tables for duplicate rows
    Reports findings and suggests cleanup if needed
    """
    try:
        with conn.cursor() as cur:
            print(f"\n🔍 Duplicate Detection Report:")
            
            # Check 1: Landing layer duplicates
            cur.execute("""
                SELECT source_name, job_id, COUNT(*) as dup_count
                FROM landing.raw_jobs
                GROUP BY source_name, job_id
                HAVING COUNT(*) > 1
                ORDER BY dup_count DESC
            """)
            landing_dups = cur.fetchall()
            
            if landing_dups:
                total_landing_dups = sum(count - 1 for _, _, count in landing_dups)
                print(f"   ⚠️  landing.raw_jobs: {len(landing_dups)} duplicate combinations, {total_landing_dups} extra rows")
                for source, job_id, count in landing_dups[:3]:
                    print(f"      {source}/{job_id}: {count} copies")
            else:
                print(f"   ✅ landing.raw_jobs: No duplicates")
            
            # Check 2: Staging jobs duplicates
            cur.execute("""
                SELECT source_name, job_id, COUNT(*) as dup_count
                FROM staging.jobs_v1
                GROUP BY source_name, job_id
                HAVING COUNT(*) > 1
                ORDER BY dup_count DESC
            """)
            staging_dups = cur.fetchall()
            
            if staging_dups:
                total_staging_dups = sum(count - 1 for _, _, count in staging_dups)
                print(f"   ⚠️  staging.jobs_v1: {len(staging_dups)} duplicate combinations, {total_staging_dups} extra rows")
                for source, job_id, count in staging_dups[:3]:
                    print(f"      {source}/{job_id}: {count} copies")
            else:
                print(f"   ✅ staging.jobs_v1: No duplicates")
            
            # Check 3: Job skills duplicates (junction table integrity)
            cur.execute("""
                SELECT source_name, job_id, skill, COUNT(*) as dup_count
                FROM staging.job_skills
                GROUP BY source_name, job_id, skill
                HAVING COUNT(*) > 1
                ORDER BY dup_count DESC
            """)
            skills_dups = cur.fetchall()
            
            if skills_dups:
                total_skill_dups = sum(count - 1 for _, _, _, count in skills_dups)
                print(f"   ⚠️  staging.job_skills: {len(skills_dups)} duplicate job-skill pairs, {total_skill_dups} extra rows")
                for source, job_id, skill, count in skills_dups[:3]:
                    print(f"      {source}/{job_id}/{skill}: {count} times")
            else:
                print(f"   ✅ staging.job_skills: No duplicates")
            
            # Check 4: Dimension table consistency
            cur.execute("""
                SELECT COUNT(DISTINCT employer_id) as unique_in_jobs,
                       (SELECT COUNT(DISTINCT employer_id) FROM staging.dim_employer) as dim_count
                FROM staging.jobs_v1
                WHERE employer_id IS NOT NULL
            """)
            jobs_unique, dim_count = cur.fetchone()
            if jobs_unique == dim_count:
                print(f"   ✅ dim_employer consistency: {jobs_unique} unique employers")
            else:
                print(f"   ⚠️  dim_employer mismatch: {jobs_unique} unique vs {dim_count} in dimension")
            
            # Overall status
            if landing_dups or staging_dups or skills_dups:
                print(f"\n   ⚠️  DUPLICATE CLEANUP RECOMMENDED")
                print(f"   Run cleanup_duplicate_rows() to remove duplicate records")
            else:
                print(f"\n   ✅ All tables clean - no duplicates detected")
                
    except Exception as e:
        print(f"⚠️ Duplicate detection warning: {e}")


def cleanup_duplicate_rows(conn) -> None:
    """
    Remove duplicate rows from all tables, keeping the most recent version
    Removes duplicates from:
      - landing.raw_jobs (keeps by max ctid)
      - staging.jobs_v1 (keeps by max ctid)
      - staging.job_skills (keeps by max ctid)
    
    This function is idempotent and safe to run multiple times
    """
    try:
        with conn.cursor() as cur:
            print(f"\n🧹 Starting duplicate cleanup...\n")
            
            # Cleanup 1: landing.raw_jobs duplicates
            cur.execute("""
                DELETE FROM landing.raw_jobs
                WHERE ctid NOT IN (
                    SELECT MAX(ctid)
                    FROM landing.raw_jobs
                    GROUP BY source_name, job_id
                )
            """)
            landing_removed = cur.rowcount
            if landing_removed > 0:
                print(f"   🗑️  landing.raw_jobs: Removed {landing_removed} duplicate rows")
            
            # Cleanup 2: staging.jobs_v1 duplicates
            cur.execute("""
                DELETE FROM staging.jobs_v1
                WHERE ctid NOT IN (
                    SELECT MAX(ctid)
                    FROM staging.jobs_v1
                    GROUP BY source_name, job_id
                )
            """)
            staging_removed = cur.rowcount
            if staging_removed > 0:
                print(f"   🗑️  staging.jobs_v1: Removed {staging_removed} duplicate rows")
            
            # Cleanup 3: staging.job_skills duplicates (same job-skill pair)
            cur.execute("""
                DELETE FROM staging.job_skills
                WHERE ctid NOT IN (
                    SELECT MAX(ctid)
                    FROM staging.job_skills
                    GROUP BY source_name, job_id, skill
                )
            """)
            skills_removed = cur.rowcount
            if skills_removed > 0:
                print(f"   🗑️  staging.job_skills: Removed {skills_removed} duplicate rows")
            
            conn.commit()
            
            total_removed = landing_removed + staging_removed + skills_removed
            if total_removed > 0:
                print(f"\n   ✅ Cleanup complete: {total_removed} duplicate rows removed")
            else:
                print(f"\n   ✅ No duplicates found - tables already clean")
            
            # Verify cleanup
            print(f"\n📋 Verification after cleanup:")
            
            cur.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM landing.raw_jobs) as landing_total,
                    (SELECT COUNT(DISTINCT (source_name, job_id)) FROM landing.raw_jobs) as landing_unique,
                    (SELECT COUNT(*) FROM staging.jobs_v1) as staging_total,
                    (SELECT COUNT(DISTINCT (source_name, job_id)) FROM staging.jobs_v1) as staging_unique,
                    (SELECT COUNT(*) FROM staging.job_skills) as skills_total,
                    (SELECT COUNT(DISTINCT (source_name, job_id, skill)) FROM staging.job_skills) as skills_unique
            """)
            
            landing_total, landing_unique, staging_total, staging_unique, skills_total, skills_unique = cur.fetchone()
            
            landing_clean = landing_total == landing_unique
            staging_clean = staging_total == staging_unique
            skills_clean = skills_total == skills_unique
            
            print(f"   landing.raw_jobs: {landing_total} rows, {landing_unique} unique {('✅' if landing_clean else '⚠️')}")
            print(f"   staging.jobs_v1: {staging_total} rows, {staging_unique} unique {('✅' if staging_clean else '⚠️')}")
            print(f"   staging.job_skills: {skills_total} rows, {skills_unique} unique {('✅' if skills_clean else '⚠️')}")
            
            if landing_clean and staging_clean and skills_clean:
                print(f"\n   ✅ All tables verified clean!")
            else:
                print(f"\n   ⚠️  Some tables still have duplicates - may need manual intervention")
                
    except Exception as e:
        conn.rollback()
        print(f"❌ Duplicate cleanup failed: {e}")
        raise


def validate_job_descriptions(conn) -> None:
    """
    Validate that job descriptions are not being truncated
    Checks for suspiciously short descriptions and common truncation patterns
    """
    try:
        with conn.cursor() as cur:
            # Check description length distribution
            cur.execute("""
                SELECT 
                    COUNT(*) as total_jobs,
                    COUNT(CASE WHEN LENGTH(COALESCE(job_description, '')) = 0 THEN 1 END) as no_description,
                    COUNT(CASE WHEN LENGTH(COALESCE(job_description, '')) < 50 THEN 1 END) as very_short,
                    COUNT(CASE WHEN LENGTH(COALESCE(job_description, '')) < 200 THEN 1 END) as short,
                    COUNT(CASE WHEN LENGTH(COALESCE(job_description, '')) >= 500 THEN 1 END) as adequate,
                    MIN(LENGTH(COALESCE(job_description, ''))) as min_length,
                    MAX(LENGTH(COALESCE(job_description, ''))) as max_length,
                    ROUND(AVG(LENGTH(COALESCE(job_description, '')))::numeric, 0) as avg_length
                FROM staging.jobs_v1
                WHERE source_name = 'reed'
            """)
            
            row = cur.fetchone()
            if row:
                total, no_desc, very_short, short_desc, adequate, min_len, max_len, avg_len = row
                
                print(f"\n📝 Description Length Validation:")
                print(f"   Total jobs: {total}")
                print(f"   No description: {no_desc}")
                print(f"   Very short (<50 chars): {very_short}")
                print(f"   Short (<200 chars): {short_desc}")
                print(f"   Adequate (≥500 chars): {adequate}")
                print(f"   Range: {min_len}-{max_len} chars, Avg: {avg_len} chars")
                
                # Check for truncation at common lengths
                cur.execute("""
                    SELECT 
                        LENGTH(job_description) as len,
                        COUNT(*) as count
                    FROM staging.jobs_v1
                    WHERE source_name = 'reed' AND job_description IS NOT NULL
                    GROUP BY LENGTH(job_description)
                    HAVING COUNT(*) > 5
                    ORDER BY COUNT(*) DESC
                    LIMIT 10
                """)
                
                suspicious_lengths = cur.fetchall()
                if suspicious_lengths:
                    # Check if many descriptions end at exact same length (sign of truncation)
                    max_count = suspicious_lengths[0][1]
                    truncation_risk = any(
                        count > total * 0.05 and length in [450, 453, 500, 1000, 2000, 3000, 5000]
                        for length, count in suspicious_lengths
                    )
                    
                    if truncation_risk:
                        print(f"   ⚠️  WARNING: Truncation detected at fixed lengths!")
                        print(f"   Top lengths: {suspicious_lengths[:5]}")
                        
                        # Show details about 453-char truncation
                        cur.execute("""
                            SELECT COUNT(*)
                            FROM staging.jobs_v1
                            WHERE source_name = 'reed' AND LENGTH(COALESCE(job_description, '')) = 453
                        """)
                        truncated_453 = cur.fetchone()[0]
                        if truncated_453 > 0:
                            print(f"   📌 {truncated_453} descriptions stuck at 453 chars (Reed API limit)")
                            print(f"      This indicates detail endpoint enrichment failed for these jobs")
                            print(f"      Action: Re-run pipeline to retry detail endpoint calls")
                    else:
                        print(f"   ✅ No truncation detected")
                else:
                    print(f"   ✅ Description lengths are varied (no truncation pattern)")
                    
    except Exception as e:
        print(f"⚠️ Description validation warning: Failed to validate descriptions: {e}")


# ---------- Function entrypoint ----------

def main(mytimer: func.TimerRequest) -> None:
    fired_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"🔥 reed_ingest fired at {fired_at}")

    # Load config & validate
    try:
        print("⏳ [1/8] Loading configuration...")
        cfg = load_config()
        print(f"✅ Config loaded: API_BASE_URL={cfg['API_BASE_URL']}, SEARCH_KEYWORDS={cfg['SEARCH_KEYWORDS']}")
    except Exception as e:
        print(f"❌ reed_ingest failed loading config: {e}")
        raise

    # Pull one page first to learn total count
    try:
        print("⏳ [2/8] Fetching page 1 from Reed API...")
        first = fetch_page(
            cfg["API_BASE_URL"],
            cfg["SEARCH_KEYWORDS"],
            cfg["RESULTS_PER_PAGE"],
            page=1,
            api_key=cfg["API_KEY"],
            api_key_backup=cfg.get("API_KEY_BACKUP"),
            api_key_backup_2=cfg.get("API_KEY_BACKUP2"),
            api_key_backup_3=cfg.get("API_KEY_BACKUP3"),
            posted_by_days=cfg["POSTED_BY_DAYS"],
        )
        print("✅ Page 1 fetched successfully")
    except Exception as e:
        print(f"❌ API call (page 1) failed: {e}")
        raise

    results_key = cfg["RESULTS_KEY"]
    total_key = cfg["TOTAL_PAGES_KEY"]

    results: List[Dict[str, Any]] = list(first.get(results_key, []) or [])
    total_results = first.get(total_key)
    if isinstance(total_results, str) and total_results.isdigit():
        total_results = int(total_results)
    print(f"📊 Page 1: {len(results)} results, total pages estimate: {total_results}")

    search_key_rotation = 0  # Track key rotation for pagination

    if total_results is None:
        # Fallback: keep fetching until an empty page
        # but still report how many we saw
        print("⚠️ totalResults key missing; will fetch until an empty page is returned.")
        total_results = len(results)

    posted_filter = f" (posted in last {cfg['POSTED_BY_DAYS']} days)" if cfg["POSTED_BY_DAYS"] > 0 else " (all jobs)"
    print(f"⏳ [3/8] Pagination: fetched={len(results)} on page=1; totalResults={total_results}{posted_filter}")

    # If totalResults exists, calculate page count; else, fetch until empty.
    pages = (
        math.ceil(total_results / cfg["RESULTS_PER_PAGE"])
        if isinstance(total_results, int) and total_results >= 0
        else 1
    )
    print(f"📄 Will fetch approximately {pages} pages")

    if pages > 1:
        skipped_pages = []
        for p in range(2, pages + 1):
            # Check MAX_RESULTS limit (0 = unlimited; >0 = cap)
            max_results = cfg.get("MAX_RESULTS", 0)
            if max_results > 0 and len(results) >= max_results:
                print(f"⚠️ Reached MAX_RESULTS limit ({max_results}); stopping pagination.")
                break
            
            # Round-robin key rotation: use (page-1) % 4 to distribute load
            key_rotation = (p - 1) % 4
            
            try:
                page_obj = fetch_page(
                    cfg["API_BASE_URL"],
                    cfg["SEARCH_KEYWORDS"],
                    cfg["RESULTS_PER_PAGE"],
                    page=p,
                    api_key=cfg["API_KEY"],
                    api_key_backup=cfg.get("API_KEY_BACKUP"),
                    api_key_backup_2=cfg.get("API_KEY_BACKUP2"),
                    api_key_backup_3=cfg.get("API_KEY_BACKUP3"),
                    posted_by_days=cfg["POSTED_BY_DAYS"],
                    key_rotation_index=key_rotation,
                )
                page_items = page_obj.get(results_key, []) or []
                if not page_items:
                    print(f"🔚 Empty page at p={p}; stopping pagination.")
                    break

                results.extend(page_items)
                progress_pct = (len(results) / total_results * 100) if isinstance(total_results, int) else 0
                print(f"  Page {p} (Key #{key_rotation + 1}): +{len(page_items)} items (total: {len(results)}, ~{progress_pct:.0f}%)")
            except Exception as e:
                # Log failed page but continue pagination instead of breaking
                print(f"⚠️ API call (page {p}) failed: {e}; skipping and continuing to next page")
                skipped_pages.append(p)
                continue
        
        if skipped_pages:
            print(f"ℹ️ Skipped {len(skipped_pages)} pages due to API errors: {skipped_pages}")

    # Apply title-based filtering using ML classifier + rule-based fallback
    print(f"⏳ [4/8] Applying title filters...")
    def _title_of(j: Dict[str, Any]) -> str:
        return (j.get('jobTitle') or j.get('title') or "").lower()
    
    pre_filter_count = len(results)
    filtered_results = []
    use_ml = cfg.get("USE_ML_CLASSIFIER", False) and ML_CLASSIFIER_AVAILABLE
    
    # DEBUG
    print(f"  DEBUG: USE_ML_CLASSIFIER={cfg.get('USE_ML_CLASSIFIER')}, ML_CLASSIFIER_AVAILABLE={ML_CLASSIFIER_AVAILABLE}, use_ml={use_ml}")
    
    if use_ml:
        # Use ML classifier
        print(f"  Using ML classifier for filtering...")
        try:
            classifier = JobClassifier()
            ml_threshold = float(cfg.get("ML_CLASSIFIER_THRESHOLD", "0.7"))
            
            for j in results:
                title = _title_of(j)
                if not title:
                    continue
                
                is_relevant, confidence = classifier.classify(title, threshold=ml_threshold)
                if is_relevant:
                    filtered_results.append(j)
            
            print(f"✅ ML Title filter: kept {len(filtered_results):,} of {pre_filter_count:,} (threshold={ml_threshold})")
        except Exception as e:
            print(f"⚠️  ML classifier failed ({e}); falling back to rule-based filtering...")
            use_ml = False
    
    if not use_ml:
        # Fallback: Rule-based filtering with word boundaries
        include_terms = cfg.get("JOB_TITLE_INCLUDE", [])
        exclude_terms = cfg.get("JOB_TITLE_EXCLUDE", [])
        
        for j in results:
            t = _title_of(j)
            if not t:
                continue
            
            # Check exclude terms first
            if exclude_terms:
                exclude_match = False
                for term in exclude_terms:
                    pattern = r'\b' + re.escape(term) + r'\b'
                    if re.search(pattern, t):
                        exclude_match = True
                        break
                if exclude_match:
                    continue
            
            # Check include terms
            if include_terms:
                include_match = False
                for term in include_terms:
                    pattern = r'\b' + re.escape(term) + r'\b'
                    if re.search(pattern, t):
                        include_match = True
                        break
                if not include_match:
                    continue
            
            filtered_results.append(j)
        
        print(f"✅ Rule-based filter: kept {len(filtered_results):,} of {pre_filter_count:,}")

    # Apply expiration date filtering - exclude jobs that have already expired
    print(f"⏳ [5/8] Checking job expiry dates...")
    pre_expiry_filter = len(filtered_results)
    now = datetime.now(timezone.utc)
    expiry_filtered_results = []
    for j in filtered_results:
        expiry_str = j.get(cfg["EXPIRY_DATE_KEY"])
        if expiry_str:
            expiry_date = parse_iso(expiry_str)
            if expiry_date and expiry_date < now:
                # Skip expired jobs
                continue
        expiry_filtered_results.append(j)
    
    expired_count = pre_expiry_filter - len(expiry_filtered_results)
    if expired_count > 0:
        print(f"✅ Expiry filter: removed {expired_count:,} expired jobs; {len(expiry_filtered_results):,} active")
    else:
        print(f"✅ Expiry filter: all {len(expiry_filtered_results):,} jobs are active")
    
    filtered_results = expiry_filtered_results

    if not filtered_results:
        print("ℹ️ No results after filtering; nothing to upsert.")
        return

    # Optionally enrich truncated descriptions and fetch additional detail fields
    # (contractType, fullTime, partTime, salaryType only available in detail endpoint)
    print(f"⏳ [6/8] Enriching job descriptions via detail endpoint...")
    enriched = []
    enriched_count = 0
    not_enriched_count = 0
    skipped_count = 0
    detail_key_rotation = 0
    for i, j in enumerate(filtered_results, 1):
        job_id_str = str(j.get(cfg['JOB_ID_KEY']))
        desc = (j.get('jobDescription') or '').strip()
        # Reed API truncates descriptions at various lengths; detect multiple truncation patterns:
        # 1. Exactly 453 characters (strong indicator from Reed search endpoint truncation)
        # 2. Ends with "..." or "…"
        # 3. Ends with common truncation phrases like "more", "click here", "apply now"
        # 4. Very short descriptions (< 200 chars) are likely truncated
        truncation_patterns = [
            r'\.\.\.$',  # ends with ...
            r'…$',  # ends with …
            r'\b(?:more|click|apply|read|view|visit|download|apply now|learn more)$',  # common truncation phrases
        ]
        is_truncated = (
            len(desc) == 453  # Reed search endpoint standard truncation length
            or len(desc) < 200  # Very short descriptions likely truncated
            or any(re.search(p, desc.lower()) for p in truncation_patterns)  # Pattern matches
        )
        
        # Fetch detail endpoint ONLY if description is truly truncated (>500 chars = good enough)
        # Skip enrichment for already-full descriptions to preserve API quota
        needs_detail = is_truncated and len(desc) < 500  # Only fetch if actually truncated
        
        # Skip enrichment if explicitly disabled or if API appears rate-limited
        skip_enrichment = cfg.get('SKIP_ENRICHMENT', False)
        
        if needs_detail and cfg.get('API_BASE_URL') and not skip_enrichment:
            try:
                # Add throttling between detail calls to reduce rate limit pressure
                if i > 1:
                    time.sleep(1.0)
                
                # Fetch with 4-tier API key fallback chain
                full = fetch_job_detail(
                    cfg['API_BASE_URL'],
                    job_id_str,
                    cfg['API_KEY'],
                    api_key_backup_1=cfg.get('API_KEY_BACKUP'),
                    api_key_backup_2=cfg.get('API_KEY_BACKUP2'),
                    api_key_backup_3=cfg.get('API_KEY_BACKUP3'),
                    key_rotation_index=detail_key_rotation,
                    backoff_seconds=2.0
                )
                detail_key_rotation += 1
                
                if full:
                    # Always merge detail-only fields (contractType, fullTime, partTime, salaryType)
                    j['contractType'] = full.get('contractType')
                    j['fullTime'] = full.get('fullTime')
                    j['partTime'] = full.get('partTime')
                    j['salaryType'] = full.get('salaryType')
                    
                    # Update description if detail has longer version
                    if full.get('jobDescription'):
                        full_desc = full.get('jobDescription', '').strip()
                        if len(full_desc) > len(desc):
                            j['jobDescription'] = full_desc
                    
                    enriched_count += 1
                    if i % 50 == 0:
                        print(f"  Enriched {enriched_count}/{i} jobs...")
                else:
                    not_enriched_count += 1

            except Exception as e:
                print(f"⚠️ Failed to enrich job {job_id_str}: {e}")
                not_enriched_count += 1
        enriched.append(j)
    print(f"✅ Enrichment complete: {enriched_count:,} descriptions updated, {not_enriched_count} failed")

    # Prepare rows
    print(f"⏳ [7/8] Preparing data for database...")
    try:
        rows = [shape_row(cfg, j) for j in enriched if cfg["JOB_ID_KEY"] in j]
        print(f"  Shaped {len(rows):,} rows")
        
        # Deduplicate by (source_name, job_id) - keep last occurrence
        # Row structure: (source_name, job_id, title, employer, location, posted_at, expires_at, date_modified, raw, content_hash)
        unique_rows = {}
        for row in rows:
            key = (row[0], row[1])  # (source_name, job_id)
            unique_rows[key] = row
        
        rows = list(unique_rows.values())
        print(f"  Deduplicated to {len(rows):,} unique jobs")
    except Exception as e:
        print(f"❌ Failed shaping rows: {e}")
        raise

    if not rows:
        print("ℹ️ No valid rows with job_id; aborting.")
        return

    # DB work
    print(f"⏳ [8/8] Writing to database...")
    try:
        with pg_connect(cfg) as conn:
            print("  Ensuring tables exist...")
            ensure_landing_table(conn)
            ensure_staging_table(conn)
            ensure_job_skills_table(conn)
            
            # Upsert enriched jobs to landing - descriptions should be in the raw JSON now
            print(f"  Upserting {len(rows):,} jobs to landing.raw_jobs...")
            upsert_jobs(conn, rows)
            if enriched_count > 0:
                print(f"    ✅ {enriched_count:,} descriptions enriched")
            
            print(f"  Transforming and upserting to staging.jobs_v1...")
            staging_count = upsert_staging_jobs(
                conn,
                cfg["SKILL_PATTERNS"],
                cfg["SKILL_ALIASES"],
                cfg.get("JOB_TITLE_INCLUDE", []),
                cfg.get("JOB_TITLE_EXCLUDE", []),
                cfg.get("JOB_ROLE_CATEGORIES", JOB_ROLE_CATEGORIES)
            )
            print(f"    ✅ Upserted {staging_count:,} rows into staging.jobs_v1")
            
            print(f"  Running cleanup for expired jobs...")
            cleanup_expired_jobs(conn, cfg)
            print(f"    ✅ Cleanup complete")
            
            # Log enrichment and skill extraction statistics
            if cfg.get("ENABLE_ENRICHMENT_MONITORING", True):
                print(f"  Logging enrichment statistics...")
                log_enrichment_stats(conn)
                log_skill_extraction_stats(conn)
                validate_job_descriptions(conn)
                detect_and_report_duplicates(conn)
    except Exception as e:
        print(f"❌ Database step failed: {e}")
        raise

    completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"\n✅ PIPELINE COMPLETE at {completed_at}")
    print(f"   Fetched: {len(results):,} total results from API")
    print(f"   Filtered: {len(filtered_results):,} after title/expiry filters")
    print(f"   Enriched: {enriched_count:,} descriptions via detail endpoint")
    print(f"   Upserted: {len(rows):,} rows to landing.raw_jobs")
    print(f"   Staged: {staging_count:,} rows to staging.jobs_v1")
    print(f"   Duration: {(datetime.now(timezone.utc) - datetime.fromisoformat(fired_at)).total_seconds():.0f}s\n")