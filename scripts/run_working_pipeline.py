#!/usr/bin/env python3
"""Run the working framework pipeline."""

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

print("🚀 RUNNING WORKING FRAMEWORK PIPELINE")
print("=" * 60)

# Upload the working pipeline notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/working_framework_pipeline.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/working_framework_pipeline",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

print("✅ Pipeline notebook uploaded")

# Create and run job
job = w.jobs.create(
    name=f"Working Framework Pipeline - {os.environ.get('USER', 'manual')}",
    tasks=[
        jobs.Task(
            task_key="working_pipeline",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/working_framework_pipeline"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Pipeline started: Run ID {run.run_id}")
print(f"📊 Monitor at: {os.getenv('DATABRICKS_HOST')}#job/{job.job_id}/run/{run.run_id}")

print("\n📋 This pipeline will:")
print("   1. Use framework code directly (no package install issues)")
print("   2. Process ALL CSV files from products directory")
print("   3. Process ALL Excel files from sales directory")
print("   4. Create Bronze, Silver, and Gold tables")
print("   5. Test dynamic file processing")
print("   6. Track all processed files")

print("\n✅ Pipeline running - check Databricks UI for progress")