-- Ensure mart schema exists
CREATE SCHEMA IF NOT EXISTS mart;

-- Helpful indexes for staging.job_skills lookups
CREATE INDEX IF NOT EXISTS idx_job_skills_skill ON staging.job_skills (skill);
CREATE INDEX IF NOT EXISTS idx_job_skills_category ON staging.job_skills (category);
CREATE INDEX IF NOT EXISTS idx_job_skills_source_job ON staging.job_skills (source_name, job_id);

-- Skill counts by category and skill
CREATE OR REPLACE VIEW mart.v_job_skill_counts AS
SELECT
  js.category,
  js.skill,
  COUNT(DISTINCT js.job_id) AS job_count,
  MIN(js.extracted_at) AS first_seen,
  MAX(js.extracted_at) AS last_seen
FROM staging.job_skills js
GROUP BY js.category, js.skill
ORDER BY job_count DESC, js.category, js.skill;

-- Optional: per-category coverage summary
CREATE OR REPLACE VIEW mart.v_job_category_coverage AS
SELECT
  category,
  COUNT(DISTINCT job_id) AS jobs_with_category,
  COUNT(*) AS pair_rows,
  MAX(extracted_at) AS last_seen
FROM staging.job_skills
GROUP BY category
ORDER BY jobs_with_category DESC;
