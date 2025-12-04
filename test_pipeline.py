#!/usr/bin/env python3
"""Test script to run the pipeline locally"""
import os
import sys
from datetime import datetime
from unittest.mock import Mock

# Configure environment
os.environ['API_KEY'] = 'ff3b8ce7-3225-4336-911f-3b1ecad843ec'
os.environ['API_BASE_URL'] = 'https://www.reed.co.uk/api/1.0/search'
os.environ['SEARCH_KEYWORDS'] = 'data,analytics,intelligence,insights'
os.environ['POSTED_BY_DAYS'] = '1'
os.environ['MAX_RESULTS'] = '10000'
os.environ['RESULTS_PER_PAGE'] = '50'
os.environ['JOB_TITLE_INCLUDE'] = 'data,bi,analyst,fabric'
os.environ['JOB_TITLE_EXCLUDE'] = 'trainee,intern,apprentice'
os.environ['PGHOST'] = 'rg-jobs.postgres.database.azure.com'
os.environ['PGPORT'] = '5432'
os.environ['PGDATABASE'] = 'postgres'
os.environ['PGUSER'] = 'gbadmin'
os.environ['PGPASSWORD'] = os.getenv('PGPASSWORD', '')
os.environ['PGSSLMODE'] = 'require'
os.environ['SOURCE_NAME'] = 'reed'
os.environ['ENABLE_EXPIRATION_CLEANUP'] = 'true'
os.environ['ENABLE_ENRICHMENT_MONITORING'] = 'true'

print("=" * 80)
print("REED JOB PIPELINE - LOCAL TEST RUN")
print("=" * 80)
print(f"Started at: {datetime.now().isoformat()}\n")

try:
    # Import after env is set
    from reed_ingest import main
    import azure.functions as func
    
    # Create mock timer
    timer = Mock(spec=func.TimerRequest)
    timer.past_due = False
    
    print("🚀 Pipeline starting...\n")
    print("-" * 80)
    
    # Run the pipeline
    main(timer)
    
    print("-" * 80)
    print(f"\n✅ Pipeline completed successfully at {datetime.now().isoformat()}")
    
except Exception as e:
    print("-" * 80)
    print(f"\n❌ Pipeline failed with error:")
    print(f"{type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
