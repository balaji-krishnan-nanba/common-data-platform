#!/usr/bin/env python3
"""Run diagnostic to find framework issues."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Check required environment variables
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

w = WorkspaceClient()

print("🔍 Running Framework Diagnostic")
print("=" * 50)

# Upload diagnostic notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/diagnose_framework_issue.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/diagnose_framework_issue",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Run diagnostic
job = w.jobs.create(
    name="Diagnose Framework Issues",
    tasks=[
        jobs.Task(
            task_key="diagnose",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/diagnose_framework_issue"
            ),
            existing_cluster_id='0721-134254-9s0ph6e7'
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Diagnostic started: Run ID {run.run_id}")
print(f"📊 Monitor at: https://adb-2908121449961741.1.azuredatabricks.net/#job/{job.job_id}/run/{run.run_id}")
print("\n⏳ Check the notebook output for diagnostic results")