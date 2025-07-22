#!/usr/bin/env python3
"""Monitor active Databricks job and show cell-level results."""

import os
import time
import json
from databricks.sdk import WorkspaceClient

# Environment variables should be set before running:
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'

w = WorkspaceClient()

print("🔍 MONITORING ACTIVE DATABRICKS JOBS")
print("=" * 60)

# Get recent runs
runs = w.jobs.list_runs(
    active_only=True,
    limit=10
)

active_runs = list(runs)
if not active_runs:
    # Get all recent runs
    runs = w.jobs.list_runs(
        limit=10
    )
    active_runs = list(runs)

print(f"\nFound {len(active_runs)} recent runs")

# Monitor the most recent cell monitor run
cell_monitor_run = None
for run in active_runs:
    if run.run_name and "Cell Monitor" in run.run_name:
        cell_monitor_run = run
        break

if not cell_monitor_run:
    # Take the most recent run
    cell_monitor_run = active_runs[0] if active_runs else None

if cell_monitor_run:
    print(f"\n📊 Monitoring Run: {cell_monitor_run.run_id}")
    print(f"   Job: {cell_monitor_run.run_name}")
    print(f"   State: {cell_monitor_run.state.life_cycle_state}")
    
    # Wait for completion
    while cell_monitor_run.state.life_cycle_state in ["PENDING", "RUNNING"]:
        print(f"   ⏳ Still running... checking again in 10s")
        time.sleep(10)
        cell_monitor_run = w.jobs.get_run(run_id=cell_monitor_run.run_id)
    
    print(f"\n✅ Run completed: {cell_monitor_run.state.result_state}")
    
    # Get output
    try:
        output = w.jobs.get_run_output(run_id=cell_monitor_run.run_id)
        if output.notebook_output and output.notebook_output.result:
            result = json.loads(output.notebook_output.result)
            print(f"\n📈 CELL EXECUTION RESULTS:")
            print(f"   Status: {result['status']}")
            print(f"   Total Cells: {result['total_cells']}")
            print(f"   Successful: {result['successful_cells']}")
            print(f"   Failed: {result['failed_cells']}")
            print(f"   Success Rate: {result['success_rate']}")
            print(f"   Duration: {result['duration_seconds']:.2f} seconds")
            
            if result['critical_failures'] > 0:
                print(f"   ⚠️ Critical Failures: {result['critical_failures']}")
    except Exception as e:
        print(f"Could not parse output: {str(e)}")

print(f"\n🔗 View in Databricks: {os.getenv('DATABRICKS_HOST')}#job/{cell_monitor_run.job_id}/run/{cell_monitor_run.run_id}")