#!/usr/bin/env python3
"""Run the framework-based E2E pipeline."""

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

print("🚀 RUNNING FRAMEWORK-BASED E2E PIPELINE")
print("=" * 60)

# First, upload the fix notebook
print("\n1. Uploading setup notebook...")
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/fix_package_installation.py", "r") as f:
    fix_content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/fix_package_installation",
    content=base64.b64encode(fix_content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Upload the main pipeline notebook
print("2. Uploading pipeline notebook...")
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/e2e_pipeline_framework.py", "r") as f:
    pipeline_content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/e2e_pipeline_framework",
    content=base64.b64encode(pipeline_content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

print("✅ Notebooks uploaded")

# Create job to run setup first
print("\n3. Running package setup...")
setup_job = w.jobs.create(
    name="Setup Package",
    tasks=[
        jobs.Task(
            task_key="setup",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/fix_package_installation"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

setup_run = w.jobs.run_now(job_id=setup_job.job_id)
print(f"✅ Setup started: Run ID {setup_run.run_id}")

# Wait for setup to complete
print("⏳ Waiting for setup to complete...")
while True:
    run_info = w.jobs.get_run(run_id=setup_run.run_id)
    if run_info.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        if run_info.state.result_state == "SUCCESS":
            print("✅ Setup completed successfully")
        else:
            print(f"❌ Setup failed: {run_info.state.state_message}")
        break
    time.sleep(10)

# Clean up setup job
try:
    w.jobs.delete(job_id=setup_job.job_id)
except:
    pass

# Run main pipeline
print("\n4. Running framework pipeline...")
pipeline_job = w.jobs.create(
    name=f"Framework E2E Pipeline - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="e2e_framework",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/e2e_pipeline_framework",
                base_parameters={
                    "AZURE_SOURCE_STORAGE_ACCOUNT": "agentstge",
                    "PROJECT_CODE": "cddp",
                    "ENVIRONMENT": "dev"
                }
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

pipeline_run = w.jobs.run_now(job_id=pipeline_job.job_id)
print(f"✅ Pipeline started: Run ID {pipeline_run.run_id}")
print(f"📊 Monitor at: https://adb-2908121449961741.1.azuredatabricks.net/#job/{pipeline_job.job_id}/run/{pipeline_run.run_id}")
print("\n⏳ Pipeline will run for ~10-15 minutes")
print("   This will:")
print("   - Clone/update the GitHub repository")
print("   - Install the framework package")
print("   - Process all CSV and Excel files")
print("   - Create Bronze, Silver, and Gold tables")
print("   - Run all transformations")