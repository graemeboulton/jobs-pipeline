#!/usr/bin/env python3
"""
Utility script to enrich truncated job descriptions by fetching full content from Reed API detail endpoint.
Run this to backfill descriptions for any jobs that still have truncated content.
"""

import os
import json
import psycopg2
import requests
from typing import Optional, Dict, Any

# Load config
def load_config() -> Dict[str, Any]:
    with open('local.settings.json') as f:
        config = json.load(f)
    return config['Values']

def pg_connect(cfg: Dict[str, Any]):
    return psycopg2.connect(
        host=cfg["PGHOST"],
        port=int(cfg["PGPORT"]),
        dbname=cfg["PGDATABASE"],
        user=cfg["PGUSER"],
        password=cfg["PGPASSWORD"],
        sslmode=cfg.get("PGSSLMODE", "require"),
    )

def fetch_job_detail(api_base_url: str, job_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch full job details from Reed API"""
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

def enrich_descriptions():
    """Find and enrich truncated descriptions"""
    cfg = load_config()
    conn = pg_connect(cfg)
    cur = conn.cursor()
    
    # Find all jobs with descriptions <= 500 chars (likely truncated)
    cur.execute("""
        SELECT j.job_id, LENGTH(j.job_description) as desc_len
        FROM staging.jobs_v1 j
        WHERE j.source_name = 'reed'
          AND LENGTH(j.job_description) <= 500
        ORDER BY LENGTH(j.job_description) ASC
        LIMIT 100
    """)
    
    truncated_jobs = cur.fetchall()
    print(f"Found {len(truncated_jobs)} jobs with potentially truncated descriptions (≤500 chars)")
    
    enriched_count = 0
    for job_id, desc_len in truncated_jobs:
        try:
            # Fetch full description from detail endpoint
            full = fetch_job_detail(cfg['API_BASE_URL'], job_id, cfg['API_KEY'])
            if full and full.get('jobDescription'):
                full_desc = full.get('jobDescription', '').strip()
                if len(full_desc) > desc_len:
                    # Update landing.raw_jobs
                    cur.execute("""
                        UPDATE landing.raw_jobs
                        SET raw = jsonb_set(raw, '{jobDescription}', to_jsonb(%s::text))
                        WHERE source_name = %s AND job_id = %s
                    """, (full_desc, 'reed', job_id))
                    
                    # Update staging.jobs_v1
                    cur.execute("""
                        UPDATE staging.jobs_v1
                        SET job_description = %s, updated_at = NOW()
                        WHERE source_name = %s AND job_id = %s
                    """, (full_desc, 'reed', job_id))
                    
                    conn.commit()
                    enriched_count += 1
                    print(f"✓ {job_id}: enriched from {desc_len} → {len(full_desc)} chars")
                else:
                    print(f"⚠️ {job_id}: detail endpoint also returned truncated ({len(full_desc)} chars)")
            else:
                print(f"⚠️ {job_id}: could not fetch detail endpoint")
        except Exception as e:
            print(f"❌ {job_id}: {e}")
    
    print(f"\n✨ Enrichment complete: {enriched_count} descriptions updated")
    conn.close()

if __name__ == '__main__':
    enrich_descriptions()
