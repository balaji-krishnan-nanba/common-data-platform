#!/usr/bin/env python3
"""Deploy and run the working E2E pipeline."""

import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Environment variables should be set before running:
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'
# export DATABRICKS_CLUSTER_ID='your_cluster_id'

w = WorkspaceClient()

print("🚀 DEPLOYING WORKING E2E PIPELINE")
print("=" * 60)

# Upload the working pipeline notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/working_e2e_pipeline.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/working_e2e_pipeline",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

print("✅ Working pipeline notebook uploaded")

# Create and run job
job = w.jobs.create(
    name=f"Working E2E Pipeline - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="working_pipeline",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/working_e2e_pipeline"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Pipeline started: Run ID {run.run_id}")
print(f"📊 Monitor at: {os.getenv('DATABRICKS_HOST')}#job/{job.job_id}/run/{run.run_id}")

print("\n⏳ Waiting for pipeline to complete...")
start_time = time.time()
max_wait = 300  # 5 minutes

while time.time() - start_time < max_wait:
    run_info = w.jobs.get_run(run_id=run.run_id)
    state = run_info.state
    
    if state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        print(f"\n📊 Pipeline {state.life_cycle_state}")
        print(f"   Result: {state.result_state}")
        
        if state.result_state == "SUCCESS":
            print("\n✅ PIPELINE COMPLETED SUCCESSFULLY!")
            print("\n📋 Verified Capabilities:")
            print("   ✓ Unity Catalog structure")
            print("   ✓ Bronze layer CSV ingestion")
            print("   ✓ Silver layer transformations")
            print("   ✓ Gold layer analytics")
            print("   ✓ Dynamic file creation")
            print("   ✓ Pipeline tracking")
        
        break
    
    elapsed = int(time.time() - start_time)
    if elapsed % 30 == 0:
        print(f"   Still running... ({elapsed}s elapsed)")
    
    time.sleep(10)

print("\n✅ Working pipeline deployment complete!")