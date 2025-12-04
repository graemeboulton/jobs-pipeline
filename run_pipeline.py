#!/usr/bin/env python3
import json
import os
import sys
from datetime import datetime
from unittest.mock import Mock

# Load settings from local.settings.json
with open('local.settings.json', 'r') as f:
    settings = json.load(f)['Values']
    for key, value in settings.items():
        os.environ[key] = value

print("=" * 80)
print("REED JOB PIPELINE - LOCAL TEST RUN")
print("=" * 80)
print(f"Started at: {datetime.now().isoformat()}\n")
print(f"Config loaded from local.settings.json")
print(f"Database: {os.environ['PGHOST']}")
print(f"Search keywords: {os.environ['SEARCH_KEYWORDS']}")
print(f"Posted by days: {os.environ['POSTED_BY_DAYS']}")
print()

try:
    from reed_ingest import main
    import azure.functions as func
    
    timer = Mock(spec=func.TimerRequest)
    timer.past_due = False
    
    print("🚀 Pipeline starting...\n")
    print("-" * 80 + "\n")
    
    main(timer)
    
    print("\n" + "-" * 80)
    print(f"✅ Pipeline completed at {datetime.now().isoformat()}")
    
except Exception as e:
    print("\n" + "-" * 80)
    print(f"❌ Pipeline failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
