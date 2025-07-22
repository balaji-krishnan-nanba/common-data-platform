#!/usr/bin/env python3
"""Run comprehensive framework validation."""

import os
import time
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Environment variables should be set before running:
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'
# export DATABRICKS_CLUSTER_ID='your_cluster_id'

w = WorkspaceClient()

print("🎯 RUNNING COMPREHENSIVE FRAMEWORK VALIDATION")
print("=" * 60)

# Upload validation notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/comprehensive_validation.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/comprehensive_validation",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

print("✅ Validation notebook uploaded")

# Create and run job
job = w.jobs.create(
    name=f"Comprehensive Validation - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="validate",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/comprehensive_validation"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Validation started: Run ID {run.run_id}")
print(f"📊 Monitor at: {os.getenv('DATABRICKS_HOST')}#job/{job.job_id}/run/{run.run_id}")

print("\n⏳ Waiting for validation to complete...")
start_time = time.time()
max_wait = 600  # 10 minutes

while time.time() - start_time < max_wait:
    run_info = w.jobs.get_run(run_id=run.run_id)
    state = run_info.state
    
    if state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        print(f"\n📊 Validation {state.life_cycle_state}")
        
        if state.result_state == "SUCCESS":
            # Get output
            try:
                output = w.jobs.get_run_output(run_id=run.run_id)
                if output.notebook_output and output.notebook_output.result:
                    result = json.loads(output.notebook_output.result)
                    print(f"\n🎯 VALIDATION RESULTS:")
                    print(f"   Status: {result['status']}")
                    print(f"   Success Rate: {result['success_rate']}")
                    print(f"   Total Passed: {result['total_passed']}")
                    print(f"   Total Failed: {result['total_failed']}")
                    print(f"   Timestamp: {result['timestamp']}")
                    
                    if result['status'] == "SUCCESS":
                        print("\n✅ FRAMEWORK FULLY VALIDATED!")
                    elif result['status'] == "PARTIAL_SUCCESS":
                        print("\n⚠️ FRAMEWORK PARTIALLY OPERATIONAL")
                    else:
                        print("\n❌ FRAMEWORK VALIDATION FAILED")
            except Exception as e:
                print(f"Error parsing output: {str(e)}")
        else:
            print(f"❌ Validation failed: {state.result_state}")
        
        break
    
    elapsed = int(time.time() - start_time)
    if elapsed % 30 == 0:
        print(f"   Still running... ({elapsed}s elapsed)")
    
    time.sleep(10)

print("\n✅ Validation complete!")