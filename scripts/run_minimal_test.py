#!/usr/bin/env python3
"""Run minimal test to debug INTERNAL_ERROR."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Environment variables should be set before running:
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'

w = WorkspaceClient()

print("🔍 RUNNING MINIMAL TEST PIPELINE")
print("=" * 60)

# Upload minimal test notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/minimal_test_pipeline.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/minimal_test_pipeline",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Create job without cluster_id to see if that's the issue
job = w.jobs.create(
    name="Minimal Test Pipeline",
    tasks=[
        jobs.Task(
            task_key="test",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/minimal_test_pipeline"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID', '0721-134254-9s0ph6e7')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Test started: Run ID {run.run_id}")
print(f"📊 Monitor at: {os.getenv('DATABRICKS_HOST')}#job/{job.job_id}/run/{run.run_id}")