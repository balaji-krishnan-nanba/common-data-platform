#!/usr/bin/env python3
"""Quick status check of recent runs."""

import os
import time
from databricks.sdk import WorkspaceClient

# Environment variables should be set externally
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'

# Initialize client
w = WorkspaceClient()

print("📊 Checking Recent Pipeline Runs...")
print("=" * 60)

# Get recent runs
runs = w.jobs.list_runs(limit=10)
current_time = time.time() * 1000

print("\nRecent Runs (Last 2 Hours):")
found_runs = False

for run in runs:
    # Check if run is from last 2 hours
    if run.start_time and run.start_time > (current_time - 7200000):  # 2 hours in milliseconds
        found_runs = True
        status_icon = "🟢" if run.state.result_state == "SUCCESS" else "🔴" if run.state.result_state == "FAILED" else "🟡"
        
        run_time = (current_time - run.start_time) / 60000  # Convert to minutes
        
        print(f"\n{status_icon} {run.run_name}")
        print(f"   Run ID: {run.run_id}")
        print(f"   Status: {run.state.life_cycle_state} - {run.state.result_state or 'IN PROGRESS'}")
        print(f"   Started: {int(run_time)} minutes ago")
        
        if run.state.state_message:
            print(f"   Message: {run.state.state_message}")

if not found_runs:
    print("\n❌ No recent runs found in the last 2 hours")

print("\n" + "=" * 60)