import os
import json
import math
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import azure.functions as func
import requests
import psycopg2
import psycopg2.extras


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
    cfg["SOURCE_NAME"] = os.getenv("SOURCE_NAME", "reed")

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


# ---------- API helpers ----------

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
    api_key: str
) -> Dict[str, Any]:
    params = {
        "keywords": keywords,
        "resultsToTake": results_per_page,
        "resultsToSkip": (page - 1) * results_per_page,
    }
    # Use requests' built-in Basic Auth
    resp = requests.get(base_url, params=params, auth=(api_key, ""))
    resp.raise_for_status()
    return resp.json()


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


def stable_hash(obj: Any) -> str:
    # Hash canonical JSON to detect content changes
    enc = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(enc).hexdigest()


def shape_row(
    cfg: Dict[str, Any],
    job: Dict[str, Any]
) -> Tuple:
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

    print(f"📥 fetched={len(results)} on page=1; totalResults={total_results}")

    # If totalResults exists, calculate page count; else, fetch until empty.
    pages = (
        math.ceil(total_results / cfg["RESULTS_PER_PAGE"])
        if isinstance(total_results, int) and total_results >= 0
        else 1
    )

    if pages > 1:
        for p in range(2, pages + 1):
            try:
                page_obj = fetch_page(
                    cfg["API_BASE_URL"],
                    cfg["SEARCH_KEYWORDS"],
                    cfg["RESULTS_PER_PAGE"],
                    page=p,
                    api_key=cfg["API_KEY"],
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

    if not results:
        print("ℹ️ No results returned; nothing to upsert.")
        return

    # Prepare rows
    try:
        rows = [shape_row(cfg, j) for j in results if cfg["JOB_ID_KEY"] in j]
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
            upsert_jobs(conn, rows)
    except Exception as e:
        print(f"❌ Database step failed: {e}")
        raise

    print(f"✅ Upsert complete. committed={len(rows)} rows from source={cfg['SOURCE_NAME']}")