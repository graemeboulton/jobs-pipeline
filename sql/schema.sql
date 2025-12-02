
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.jobs_raw (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT,
    fetched_at TIMESTAMPTZ DEFAULT now(),
    payload JSONB
);

CREATE TABLE IF NOT EXISTS mart.jobs_clean (
    job_id TEXT PRIMARY KEY,
    title TEXT,
    company_name TEXT,
    location TEXT,
    salary_min NUMERIC,
    salary_max NUMERIC,
    currency TEXT,
    employment_type TEXT,
    posted_date DATE,
    url TEXT,
    last_seen_at TIMESTAMPTZ DEFAULT now()
);
