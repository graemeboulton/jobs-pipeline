-- Keep things tidy and generic
CREATE SCHEMA IF NOT EXISTS landing;

CREATE TABLE IF NOT EXISTS landing.raw_jobs (
  source        TEXT         NOT NULL,           -- e.g., "reed", "indeed", "linkedin"
  job_id        TEXT         NOT NULL,           -- string to cover any provider ID shape
  raw           JSONB        NOT NULL,           -- full provider JSON for the job
  posted_date   TIMESTAMPTZ,
  expiry_date   TIMESTAMPTZ,
  date_modified TIMESTAMPTZ,
  ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  payload_hash  TEXT         NOT NULL,
  PRIMARY KEY (source, job_id)
);

-- Change detection + fast dedupe
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_jobs_src_jobid_hash
  ON landing.raw_jobs (source, job_id, payload_hash);

-- JSON search
CREATE INDEX IF NOT EXISTS ix_raw_jobs_raw_gin
  ON landing.raw_jobs
  USING GIN (raw);

-- Common filters
CREATE INDEX IF NOT EXISTS ix_raw_jobs_posted_date ON landing.raw_jobs (posted_date);
CREATE INDEX IF NOT EXISTS ix_raw_jobs_expiry_date ON landing.raw_jobs (expiry_date);

-- Optional example view:
-- Tailor the field mapping to whichever provider you’re using.
CREATE OR REPLACE VIEW landing.v_jobs_flat_example AS
SELECT
  source,
  job_id,
  raw->>'jobTitle'             AS job_title,
  raw->>'employerName'         AS employer_name,
  (raw->>'employerId')::INT    AS employer_id,
  raw->>'locationName'         AS location_name,
  (raw->>'latitude')::FLOAT    AS latitude,
  (raw->>'longitude')::FLOAT   AS longitude,
  (raw->>'salaryMin')::NUMERIC AS salary_min,
  (raw->>'salaryMax')::NUMERIC AS salary_max,
  raw->>'salaryType'           AS salary_type,
  raw->>'contractType'         AS contract_type,
  raw->>'contractTime'         AS contract_time,
  raw->>'jobUrl'               AS job_url,
  raw->>'isRemote'             AS is_remote,
  posted_date,
  expiry_date,
  date_modified,
  ingested_at,
  updated_at
FROM landing.raw_jobs;