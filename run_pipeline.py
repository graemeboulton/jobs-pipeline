#!/usr/bin/env python3
"""
Local test runner for the Reed Job Pipeline.

This script loads configuration from local.settings.json and runs the
complete ETL pipeline (API fetch, transform, load) without deploying to Azure.

Usage:
    python run_pipeline.py

Output:
    - Fetches jobs from Reed API based on SEARCH_KEYWORDS and POSTED_BY_DAYS
    - Applies title filtering (JOB_TITLE_INCLUDE/EXCLUDE)
    - Enriches descriptions and extracts metadata
    - Loads to PostgreSQL (staging.jobs_v1, staging.job_skills)
    - Validates data quality and reports metrics

Configuration:
    - All settings from local.settings.json (environment variables)
    - Update local.settings.json to change keywords, filters, API keys, etc.
"""

import json
import os
import sys
from datetime import datetime
from unittest.mock import Mock
from pathlib import Path

# Load environment from local.settings.json
settings_path = Path('local.settings.json')
if not settings_path.exists():
    print("❌ Error: local.settings.json not found. Create it from the README template.")
    sys.exit(1)

with settings_path.open() as f:
    settings = json.load(f)['Values']
    for key, value in settings.items():
        os.environ[key] = value

# Print config summary
print("=" * 80)
print("REED JOB PIPELINE - LOCAL TEST RUN")
print("=" * 80)
print(f"Started: {datetime.now().isoformat()}\n")
print(f"Config: {settings_path.resolve()}")
print(f"Database: {os.environ.get('PGHOST')}")
print(f"Keywords: {os.environ.get('SEARCH_KEYWORDS')}")
print(f"Posted by days: {os.environ.get('POSTED_BY_DAYS')}")
print(f"Include filters: {os.environ.get('JOB_TITLE_INCLUDE')}")
print(f"Exclude filters: {os.environ.get('JOB_TITLE_EXCLUDE')[:50]}...")
print()

try:
    from reed_ingest import main
    import azure.functions as func
    
    # Mock Azure Timer context for local execution
    timer = Mock(spec=func.TimerRequest)
    timer.past_due = False
    
    print("🚀 Pipeline starting...\n")
    print("-" * 80 + "\n")
    
    # Run the main pipeline
    main(timer)
    
    print("\n" + "-" * 80)
    print(f"✅ Pipeline completed at {datetime.now().isoformat()}")
    print("=" * 80)
    
except Exception as e:
    print("\n" + "-" * 80)
    print(f"❌ Pipeline failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
