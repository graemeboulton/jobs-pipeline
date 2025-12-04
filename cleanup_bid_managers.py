#!/usr/bin/env python3
"""
Clean up jobs that were incorrectly included due to 'bi' matching 'bid'.
This removes jobs that don't actually match the inclusion criteria.
"""

import psycopg2
import time

def cleanup():
    # Retry connection with exponential backoff
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host="rg-jobs.postgres.database.azure.com",
                port=5432,
                dbname="postgres",
                user="gbadmin",
                password="Catherin3!",
                sslmode="require",
                connect_timeout=10
            )
            break
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Connection failed, retrying in {wait_time}s... ({attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise
    
    try:
        cur = conn.cursor()
        
        # Delete bid manager/bid writer jobs (they only match 'bi' as substring, not as word)
        cur.execute("""
            DELETE FROM staging.jobs_v1
            WHERE source_name = 'reed'
              AND (
                LOWER(job_title) LIKE '%bid manager%'
                OR LOWER(job_title) LIKE '%bid writer%'
                OR LOWER(job_title) LIKE '%bid finance%'
              )
        """)
        
        deleted = cur.rowcount
        conn.commit()
        print(f"✓ Deleted {deleted} bid manager/writer jobs")
        
        # Verify
        cur.execute("""
            SELECT COUNT(*)
            FROM staging.jobs_v1
            WHERE source_name = 'reed'
        """)
        remaining = cur.fetchone()[0]
        print(f"✓ Total remaining jobs: {remaining}")
        
    finally:
        conn.close()

if __name__ == '__main__':
    cleanup()
