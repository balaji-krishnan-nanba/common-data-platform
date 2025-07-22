#!/usr/bin/env python3
"""Run the direct E2E pipeline implementation."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Check environment variables
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

w = WorkspaceClient()

print("🚀 RUNNING DIRECT E2E PIPELINE")
print("=" * 60)

# Upload the direct pipeline notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/direct_e2e_pipeline.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/direct_e2e_pipeline",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

print("✅ Pipeline notebook uploaded")

# Create and run job
job = w.jobs.create(
    name="Direct E2E Pipeline - FINAL",
    tasks=[
        jobs.Task(
            task_key="direct_pipeline",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/direct_e2e_pipeline"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID', '0721-134254-9s0ph6e7')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Pipeline started: Run ID {run.run_id}")
print(f"📊 Monitor at: {os.getenv('DATABRICKS_HOST', 'https://adb-2908121449961741.1.azuredatabricks.net/')}#job/{job.job_id}/run/{run.run_id}")

print("\n📋 This pipeline will:")
print("   1. Process ALL CSV files from products directory")
print("   2. Process ALL Excel files from sales directory")
print("   3. Create Bronze layer tables with metadata")
print("   4. Create Silver layer with cleaned data")
print("   5. Create Gold layer with analytics")
print("   6. Track all processed files")
print("   7. Test dynamic file processing")

print("\n✅ Direct implementation - no package dependencies!")
print("⏳ Estimated runtime: 5-10 minutes")