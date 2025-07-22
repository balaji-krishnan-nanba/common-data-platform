#!/usr/bin/env python3
"""Check status of working pipeline run."""

import os
from databricks.sdk import WorkspaceClient

# Environment variables should be set before running:
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'

w = WorkspaceClient()

print("🔍 CHECKING WORKING PIPELINE STATUS")
print("=" * 60)

# Get recent runs
runs = w.jobs.list_runs(limit=5)
for run in runs:
    if run.run_name and "Working E2E Pipeline" in run.run_name:
        print(f"\n📊 Found Working Pipeline Run: {run.run_id}")
        print(f"   State: {run.state.life_cycle_state}")
        print(f"   Result: {run.state.result_state}")
        print(f"   URL: {os.getenv('DATABRICKS_HOST')}#job/{run.job_id}/run/{run.run_id}")
        
        if run.state.result_state == "SUCCESS":
            print("\n✅ PIPELINE SUCCESSFUL!")
            print("   All components are working correctly")
        break