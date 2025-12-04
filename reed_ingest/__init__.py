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


# ---------- Skills extraction patterns ----------

# Grouped skills to enable category tagging (e.g., programming_languages, databases)
SKILL_GROUPS: Dict[str, set] = {
    "programming_languages": {
        'python', 'java', 'javascript', 'typescript', 'c++', 'ruby', 'php', 'rust', 'swift', 'kotlin', 'scala'
        # Removed: 'c#', 'go', 'r' (too short / high false-positive rate)
    },
    "databases": {
        'sql', 'mysql', 'postgresql', 'postgres', 'mongodb', 'redis', 'elasticsearch', 'oracle', 'sql server', 'dynamodb',
        'nosql', 'cassandra', 'neo4j', 'snowflake', 'bigquery'
    },
    "cloud_devops": {
        'aws', 'azure', 'gcp', 'google cloud', 'docker', 'kubernetes', 'terraform', 'ansible', 'jenkins', 'gitlab',
        'github actions', 'ci/cd', 'circleci', 'travis ci', 'helm', 'argocd'
        # Removed: 'k8s' (< 3 chars)
    },
    "data_ml": {
        'machine learning', 'deep learning', 'nlp', 'tensorflow', 'pytorch', 'scikit-learn',
        'pandas', 'numpy', 'matplotlib', 'jupyter', 'spark', 'hadoop', 'kafka', 'airflow', 'databricks', 'mlflow'
        # Removed: 'ai' (too short / high false-positive rate)
    },
    "bi_analytics": {
        'tableau', 'power bi', 'looker', 'qlik', 'excel'
    },
    "methodologies": {
        'agile', 'scrum', 'kanban', 'devops', 'tdd', 'bdd', 'microservices', 'rest', 'restful', 'graphql', 'api'
    },
    "other_tools": {
        'git', 'jira', 'confluence', 'linux', 'bash', 'powershell', 'vim', 'vscode', 'intellij',
        'selenium', 'cypress', 'jest', 'pytest', 'junit', 'postman'
    }
}

# Flat set used for matching (backwards-compatible)
SKILL_PATTERNS = set().union(*SKILL_GROUPS.values())

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
    
    # Machine Learning variations
    'ml': 'machine learning',
    'machinelearning': 'machine learning',
    
    # Artificial Intelligence variations
    'artificial intelligence': 'ai',
    'a.i.': 'ai',
    
    # .NET variations
    'dotnet': '.net',
    'dot net': '.net',
    '.net core': '.net',
    'asp.net': '.net',
    
    # Node.js variations
    'nodejs': 'node.js',
    'node js': 'node.js',
    
    # JavaScript variations
    'js': 'javascript',
    'java script': 'javascript',
    
    # TypeScript variations
    'ts': 'typescript',
    'type script': 'typescript',
    
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
    cfg["API_BASE_URL"] = _must_get("API_BASE_URL")
    cfg["SEARCH_KEYWORDS"] = os.getenv("SEARCH_KEYWORDS", "data")
    cfg["RESULTS_PER_PAGE"] = int(os.getenv("RESULTS_PER_PAGE", "50"))
    cfg["MAX_RESULTS"] = int(os.getenv("MAX_RESULTS", "10000"))
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
    exclude_str = os.getenv("JOB_TITLE_EXCLUDE", "trainee")
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
        UNIQUE (source_name, job_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        # Ensure columns exist if table was created previously
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS work_location_type TEXT;")
        cur.execute("ALTER TABLE staging.jobs_v1 ADD COLUMN IF NOT EXISTS seniority_level TEXT;")
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


def upsert_staging_jobs(conn, skill_patterns: set = None, skill_aliases: dict = None, include_terms: List[str] = None, exclude_terms: List[str] = None):
    """
    Transform landing.raw_jobs → staging.jobs_v1
    Extracts and flattens JSON fields from raw column
    Includes skill extraction from job descriptions
    
    Args:
        conn: Database connection
        skill_patterns: Set of skills to extract (uses default if None)
        skill_aliases: Dict mapping variations to canonical names (uses default if None)
    """
    # First, fetch job descriptions that need skill extraction
    # Build filtered selection of jobs from landing.raw_jobs based on title include/exclude
    include_terms = include_terms or []
    exclude_terms = exclude_terms or []
    where_clauses = ["raw->>'jobDescription' IS NOT NULL", "raw->>'jobTitle' IS NOT NULL"]
    params: List[str] = []
    if include_terms:
        inc_pred = "(" + " OR ".join(["lower(raw->>'jobTitle') LIKE %s" for _ in include_terms]) + ")"
        where_clauses.append(inc_pred)
        params.extend([f"%{t.lower()}%" for t in include_terms])
    if exclude_terms:
        exc_pred = "NOT (" + " OR ".join(["lower(raw->>'jobTitle') LIKE %s" for _ in exclude_terms]) + ")"
        where_clauses.append(exc_pred)
        params.extend([f"%{t.lower()}%" for t in exclude_terms])
    where_sql = " AND ".join(where_clauses)
    sel_sql = f"""
        SELECT job_id, source_name, raw->>'jobTitle' as title, raw->>'jobDescription' as description
        FROM landing.raw_jobs
        WHERE {where_sql}
    """
    with conn.cursor() as cur:
        cur.execute(sel_sql, params)
        jobs = cur.fetchall()
    
    # Extract skills and detect work location type for each job
    skills_map: Dict[Tuple[str, str], List[str]] = {}
    location_map: Dict[Tuple[str, str], str] = {}
    # rows of (source_name, job_id, category, canonical)
    job_skill_pairs: List[Tuple[str, str, str, str]] = []
    # Build reverse map pattern -> category for fast lookup
    pattern_category: Dict[str, str] = {}
    for cat, patterns in SKILL_GROUPS.items():
        for p in patterns:
            pattern_category[p] = cat
    # Map to store seniority levels
    seniority_map: Dict[Tuple[str, str], str] = {}
    
    for job_id, source_name, title, description in jobs:
        # Extract skills using the updated extract_skills function (min length filter applied there)
        canonical_skills = extract_skills(description, skill_patterns, skill_aliases)
        skills_map[(source_name, job_id)] = canonical_skills
        # Detect work location type
        location_type = detect_work_location_type(title, description)
        location_map[(source_name, job_id)] = location_type
        # Detect seniority level
        seniority = detect_seniority_level(title, description)
        seniority_map[(source_name, job_id)] = seniority
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
    
    # Convert skills_map to PostgreSQL array format for bulk insert
    # This is a simplified approach - in production, you'd do this more efficiently
    with conn.cursor() as cur:
        # For now, we'll update each row individually with its skills
        # A more efficient approach would be to use a temp table or UNNEST
        for (source_name, job_id), skills in skills_map.items():
            location_type = location_map.get((source_name, job_id), 'unknown')
            seniority = seniority_map.get((source_name, job_id), 'mid')
            cur.execute("""
                INSERT INTO staging.jobs_v1 (
                    source_name, job_id, job_title, employer_name, employer_id,
                    location_name, salary_min, salary_max, job_url,
                    applications, job_description, work_location_type, seniority_level, posted_at, expires_at,
                    ingested_at, updated_at
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
                    raw->>'jobUrl' as job_url,
                    (raw->>'applications')::int as applications,
                    raw->>'jobDescription' as job_description,
                    %s as work_location_type,
                    %s as seniority_level,
                    posted_at,
                    expires_at,
                    ingested_at,
                    NOW() as updated_at
                FROM landing.raw_jobs
                WHERE source_name = %s AND job_id = %s
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
                    posted_at = EXCLUDED.posted_at,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = NOW();
            """, (location_type, seniority, source_name, job_id))
        
        row_count = len(skills_map)
    
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
    posted_by_days: Optional[int] = None
) -> Dict[str, Any]:
    """
    Fetch single page of results from Reed API search endpoint.
    
    Supports pagination via resultsToTake/resultsToSkip parameters.
    Optionally filters to jobs posted within N days via postedByDays parameter
    (0 = all jobs, >0 = incremental mode).
    
    Args:
        base_url: Reed API base URL (https://www.reed.co.uk/api/1.0/search)
        keywords: Search keyword string (comma-separated for multiple)
        results_per_page: Page size (typically 50)
        page: Page number (1-based)
        api_key: Reed API authentication key
        posted_by_days: Filter to jobs posted in last N days (optional)
        
    Returns:
        API response dict with 'results' and 'totalResults' keys
        
    Raises:
        requests.HTTPError: On API request failure
    """
    params = {
        "keywords": keywords,
        "resultsToTake": results_per_page,
        "resultsToSkip": (page - 1) * results_per_page,
    }
    
    # Add incremental filter: only fetch jobs posted in last N days
    if posted_by_days and posted_by_days > 0:
        params["postedByDays"] = posted_by_days
    
    # Use requests' built-in Basic Auth
    resp = requests.get(base_url, params=params, auth=(api_key, ""))
    resp.raise_for_status()
    return resp.json()


def fetch_job_detail(api_base_url: str, job_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetch full job details for a given job_id using Reed API.
    Tries the jobs endpoint inferred from API_BASE_URL.
    """
    # Infer jobs endpoint: replace trailing '/search' with '/jobs/{id}'
    jobs_url = api_base_url.rstrip('/')
    if jobs_url.endswith('/search'):
        jobs_url = jobs_url[:-len('/search')]
    jobs_url = f"{jobs_url}/jobs/{job_id}"
    try:
        r = requests.get(jobs_url, auth=(api_key, ""))
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"⚠️ Failed to fetch job detail for {job_id}: {e}")
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
    
    # Remote indicators (strong signals)
    remote_patterns = [
        r'\b(?:fully |100% |completely )?remote\b',
        r'\bwork from home\b',
        r'\bwfh\b',
        r'\b(?:fully |100% )?home.?based\b',
        r'\bremote.?(?:first|only|working)\b',
        r'\banywhere in (?:the )?uk\b',
    ]
    
    # Hybrid indicators
    hybrid_patterns = [
        r'\bhybrid\b',
        r'\b\d+\s*days?\s+(?:in|at|in the|per)\s+(?:the )?office\b',
        r'\b\d+\s*days?\s+(?:remote|home|wfh)\b',
        r'\bflexible working\b',
        r'\bpart.?remote\b',
        r'\bcombination of (?:remote|home) and office\b',
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


def stable_hash(obj: Any) -> str:
    # Hash canonical JSON to detect content changes
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


# ---------- Function entrypoint ----------

def main(mytimer: func.TimerRequest) -> None:
    fired_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"🔥 reed_ingest fired at {fired_at}")

    # Load config & validate
    try:
        cfg = load_config()
    except Exception as e:
        print(f"❌ reed_ingest failed loading config: {e}")
        raise

    # Pull one page first to learn total count
    try:
        first = fetch_page(
            cfg["API_BASE_URL"],
            cfg["SEARCH_KEYWORDS"],
            cfg["RESULTS_PER_PAGE"],
            page=1,
            api_key=cfg["API_KEY"],
            posted_by_days=cfg["POSTED_BY_DAYS"],
        )
    except Exception as e:
        print(f"❌ API call (page 1) failed: {e}")
        raise

    results_key = cfg["RESULTS_KEY"]
    total_key = cfg["TOTAL_PAGES_KEY"]

    results: List[Dict[str, Any]] = list(first.get(results_key, []) or [])
    total_results = first.get(total_key)
    if isinstance(total_results, str) and total_results.isdigit():
        total_results = int(total_results)

    if total_results is None:
        # Fallback: keep fetching until an empty page
        # but still report how many we saw
        print("⚠️ totalResults key missing; will fetch until an empty page is returned.")
        total_results = len(results)

    posted_filter = f" (posted in last {cfg['POSTED_BY_DAYS']} days)" if cfg["POSTED_BY_DAYS"] > 0 else " (all jobs)"
    print(f"� fetched={len(results)} on page=1; totalResults={total_results}{posted_filter}")

    # If totalResults exists, calculate page count; else, fetch until empty.
    pages = (
        math.ceil(total_results / cfg["RESULTS_PER_PAGE"])
        if isinstance(total_results, int) and total_results >= 0
        else 1
    )

    if pages > 1:
        for p in range(2, pages + 1):
            # Check MAX_RESULTS limit
            if len(results) >= cfg.get("MAX_RESULTS", float('inf')):
                print(f"⚠️ Reached MAX_RESULTS limit ({cfg.get('MAX_RESULTS')}); stopping pagination.")
                break
            try:
                page_obj = fetch_page(
                    cfg["API_BASE_URL"],
                    cfg["SEARCH_KEYWORDS"],
                    cfg["RESULTS_PER_PAGE"],
                    page=p,
                    api_key=cfg["API_KEY"],
                    posted_by_days=cfg["POSTED_BY_DAYS"],
                )
            except Exception as e:
                print(f"❌ API call (page {p}) failed: {e}")
                break

            page_items = page_obj.get(results_key, []) or []
            if not page_items:
                print(f"🔚 empty page at p={p}; stopping pagination.")
                break

            results.extend(page_items)
            print(f"📥 fetched+={len(page_items)} (running total={len(results)})")

    # Apply title-based include/exclude filtering
    def _title_of(j: Dict[str, Any]) -> str:
        return (j.get('jobTitle') or j.get('title') or "").lower()
    include_terms = cfg.get("JOB_TITLE_INCLUDE", [])
    exclude_terms = cfg.get("JOB_TITLE_EXCLUDE", [])
    pre_filter_count = len(results)
    filtered_results = []
    for j in results:
        t = _title_of(j)
        if not t:
            continue
        # Use word boundary matching for both include and exclude filters
        # This prevents "bi" from matching "bid", "spiral", etc.
        if include_terms:
            include_match = False
            for term in include_terms:
                # Match term as whole word or at word boundary
                pattern = r'\b' + re.escape(term) + r'\b'
                if re.search(pattern, t):
                    include_match = True
                    break
            if not include_match:
                continue
        
        if exclude_terms:
            exclude_match = False
            for term in exclude_terms:
                # Match term as whole word or at word boundary
                pattern = r'\b' + re.escape(term) + r'\b'
                if re.search(pattern, t):
                    exclude_match = True
                    break
            if exclude_match:
                continue
        
        filtered_results.append(j)
    print(f"🔎 title-filter: kept={len(filtered_results)} of {pre_filter_count} (include={include_terms} exclude={exclude_terms})")

    # Apply expiration date filtering - exclude jobs that have already expired
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
        print(f"⏰ expiry-filter: removed={expired_count} expired jobs; kept={len(expiry_filtered_results)} of {pre_expiry_filter}")
    
    filtered_results = expiry_filtered_results

    if not results:
        print("ℹ️ No results returned; nothing to upsert.")
        return

    # Optionally enrich truncated descriptions by fetching job detail
    enriched = []
    enriched_count = 0
    not_enriched_count = 0
    for j in filtered_results:
        job_id_str = str(j.get(cfg['JOB_ID_KEY']))
        desc = (j.get('jobDescription') or '').strip()
        # Reed API truncates descriptions at various lengths; detect multiple truncation patterns:
        # 1. Ends with "..." or "…"
        # 2. Ends with common truncation phrases like "more", "click here", "apply now"
        # 3. Very short descriptions (< 200 chars) are likely truncated
        truncation_patterns = [
            r'\.\.\.$',  # ends with ...
            r'…$',  # ends with …
            r'\b(?:more|click|apply|read|view|visit|download|apply now|learn more)$',  # common truncation phrases
        ]
        is_truncated = len(desc) < 200 or any(re.search(p, desc.lower()) for p in truncation_patterns)
        
        if is_truncated and cfg.get('API_BASE_URL'):
            try:
                full = fetch_job_detail(cfg['API_BASE_URL'], job_id_str, cfg['API_KEY'])
                if full and full.get('jobDescription'):
                    full_desc = full.get('jobDescription', '').strip()
                    if len(full_desc) > len(desc):
                        j['jobDescription'] = full_desc
                        enriched_count += 1
                        # Verify the update actually happened
                        if j.get('jobDescription') != full_desc:
                            print(f"⚠️ ERROR: Failed to update job {job_id_str} description in-memory")
                    else:
                        not_enriched_count += 1
                else:
                    not_enriched_count += 1
            except Exception as e:
                print(f"⚠️ Failed to enrich job {job_id_str}: {e}")
                not_enriched_count += 1
        enriched.append(j)
    print(f"✨ Enriched {enriched_count} job descriptions; {not_enriched_count} could not be enriched")

    # Prepare rows
    try:
        rows = [shape_row(cfg, j) for j in enriched if cfg["JOB_ID_KEY"] in j]
        print(f"🧾 prepared rows={len(rows)} (with job_id present)")
        
        # Deduplicate by (source_name, job_id) - keep last occurrence
        # Row structure: (source_name, job_id, title, employer, location, posted_at, expires_at, date_modified, raw, content_hash)
        unique_rows = {}
        for row in rows:
            key = (row[0], row[1])  # (source_name, job_id)
            unique_rows[key] = row
        
        rows = list(unique_rows.values())
        print(f"🔑 unique rows={len(rows)} (after deduplication)")
    except Exception as e:
        print(f"❌ Failed shaping rows: {e}")
        raise

    if not rows:
        print("ℹ️ No valid rows with job_id; aborting.")
        return

    # DB work
    try:
        with pg_connect(cfg) as conn:
            ensure_landing_table(conn)
            ensure_staging_table(conn)
            ensure_job_skills_table(conn)
            
            # Upsert enriched jobs to landing - descriptions should be in the raw JSON now
            upsert_jobs(conn, rows)
            if enriched_count > 0:
                print(f"💾 Persisted {enriched_count} enriched descriptions via upsert")
            
            staging_count = upsert_staging_jobs(
                conn,
                cfg["SKILL_PATTERNS"],
                cfg["SKILL_ALIASES"],
                cfg.get("JOB_TITLE_INCLUDE", []),
                cfg.get("JOB_TITLE_EXCLUDE", [])
            )
            print(f"📦 Staging: upserted {staging_count} rows into staging.jobs_v1")
            
            # Run cleanup for expired jobs
            cleanup_expired_jobs(conn, cfg)
            
            # Log enrichment and skill extraction statistics
            if cfg.get("ENABLE_ENRICHMENT_MONITORING", True):
                log_enrichment_stats(conn)
                log_skill_extraction_stats(conn)
    except Exception as e:
        print(f"❌ Database step failed: {e}")
        raise

    print(f"✅ Upsert complete. committed={len(rows)} rows from source={cfg['SOURCE_NAME']}")