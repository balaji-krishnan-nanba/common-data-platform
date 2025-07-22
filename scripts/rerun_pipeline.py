#!/usr/bin/env python3
"""Re-run pipeline to process new files."""

import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# Check required environment variables
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

w = WorkspaceClient()

print("🔄 RE-RUNNING PIPELINE FOR NEW FILES")
print("=" * 60)

# Wait a bit for files to be available
print("⏳ Waiting 30 seconds for new files to be available...")
time.sleep(30)

# Create and run pipeline job
job = w.jobs.create(
    name=f"E2E Pipeline RERUN - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="e2e_pipeline_rerun",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/test_complete_pipeline"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Pipeline RERUN started: Run ID {run.run_id}")
print(f"📊 Monitor at: https://adb-2908121449961741.1.azuredatabricks.net/#job/{job.job_id}/run/{run.run_id}")

print("\n📁 This run should process:")
print("   - Gaming products (new category)")
print("   - Food & Beverage products (new category)")
print("   - Today's sales transactions")
print("   - Special promo sales")
print("\n⏳ Pipeline will run for ~5-10 minutes")