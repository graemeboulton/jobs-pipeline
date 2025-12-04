-- Add a character length constraint to job_description
ALTER TABLE staging.jobs_v1
    ADD CONSTRAINT jobs_v1_job_description_len
    CHECK (char_length(job_description) <= 20000);
