CREATE OR REPLACE VIEW staging.dim_salaryband AS
WITH base AS (
    SELECT DISTINCT
        GREATEST(0, COALESCE(salary_min,0)) AS smin,
        GREATEST(0, COALESCE(salary_max,0)) AS smax,
        COALESCE((salary_min + salary_max)/2.0, 0) AS savg
    FROM staging.jobs_v1
)
, nums AS (
    SELECT DISTINCT LEAST(540000, (FLOOR(GREATEST(savg, (smin + smax)/2.0)/10000.0)*10000)::int) AS band_start
    FROM base
)
SELECT
    DENSE_RANK() OVER (ORDER BY band_start) AS salaryband_key,
    band_start,
    CASE
        WHEN band_start = 540000 THEN 549999
        ELSE band_start + 9999
    END                                   AS band_end,
    CASE
        WHEN band_start = 540000 THEN CONCAT('£', band_start, '–£', 549999)
        ELSE CONCAT('£', band_start, '–£', band_start + 9999)
    END                                   AS band_label,
    DENSE_RANK() OVER (ORDER BY band_start) AS sort_order
FROM nums
ORDER BY band_start;