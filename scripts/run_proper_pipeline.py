#!/usr/bin/env python3
"""Run the properly configured E2E pipeline."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64
import time

# Check required environment variables
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

w = WorkspaceClient()

print("🚀 RUNNING PROPER FRAMEWORK-BASED E2E PIPELINE")
print("=" * 60)

# Upload the proper pipeline notebook
print("\n1. Uploading pipeline notebook...")
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/e2e_pipeline_proper.py", "r") as f:
    pipeline_content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/e2e_pipeline_proper",
    content=base64.b64encode(pipeline_content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)
print("✅ Pipeline notebook uploaded")

# Create and run the pipeline job
print("\n2. Creating pipeline job...")
pipeline_job = w.jobs.create(
    name=f"Proper E2E Pipeline - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="e2e_pipeline",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/e2e_pipeline_proper"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID'),
        )
    ]
)

pipeline_run = w.jobs.run_now(job_id=pipeline_job.job_id)
print(f"✅ Pipeline started: Run ID {pipeline_run.run_id}")
print(f"📊 Monitor at: https://adb-2908121449961741.1.azuredatabricks.net/#job/{pipeline_job.job_id}/run/{pipeline_run.run_id}")

print("\n📋 This pipeline will:")
print("   1. Install package from Databricks Repo")
print("   2. Use correct imports from common_data_platform")
print("   3. Load configuration from YAML files")
print("   4. Process all CSV and Excel files dynamically")
print("   5. Create Bronze, Silver, and Gold tables")
print("   6. Validate all layers")

print("\n⏳ Estimated runtime: 10-15 minutes")