#!/usr/bin/env python3
"""Query cell execution results from monitoring table."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Environment variables should be set before running:
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'
# export DATABRICKS_CLUSTER_ID='your_cluster_id'

w = WorkspaceClient()

print("📊 QUERYING CELL EXECUTION RESULTS")
print("=" * 60)

# Upload query notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/query_cell_results.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/query_cell_results",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Run query
job = w.jobs.create(
    name="Query Cell Results",
    tasks=[
        jobs.Task(
            task_key="query",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/query_cell_results"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Query started: Run ID {run.run_id}")
print(f"📊 View at: {os.getenv('DATABRICKS_HOST')}#job/{job.job_id}/run/{run.run_id}")