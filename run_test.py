#!/usr/bin/env python3
import json
import os
from datetime import datetime
from unittest.mock import Mock

# Load settings
with open('local.settings.json', 'r') as f:
    settings = json.load(f)['Values']
    for key, value in settings.items():
        os.environ[key] = value

# Limit results for testing
os.environ['MAX_RESULTS'] = '100'  # Test with smaller batch

print("=" * 80)
print("REED JOB PIPELINE - TEST RUN")
print("=" * 80)
print(f"Started: {datetime.now().isoformat()}")
print(f"Config: MAX_RESULTS=100 (limited for testing)\n")

from reed_ingest import main
import azure.functions as func

timer = Mock(spec=func.TimerRequest)
timer.past_due = False

print("-" * 80 + "\n")
main(timer)
print("\n" + "-" * 80)
print(f"✅ Completed: {datetime.now().isoformat()}\n")
