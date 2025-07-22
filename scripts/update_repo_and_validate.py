#!/usr/bin/env python3
"""Update Databricks repo and run complete validation."""

import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Check required environment variables
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

w = WorkspaceClient()

print("🔄 UPDATE REPO AND VALIDATE FRAMEWORK")
print("=" * 60)

# Step 1: Update Databricks Repo
print("\n1. Updating Databricks Repo...")
repo_path = "/Repos/balaji.krishnan@nanba.co.uk/common-data-platform"

try:
    # Find the repo
    repos = w.repos.list(path_prefix="/Repos/balaji.krishnan@nanba.co.uk/")
    repo_id = None
    
    for repo in repos:
        if repo.path == repo_path:
            repo_id = repo.id
            break
    
    if repo_id:
        # Update to latest
        print(f"   Updating repo ID: {repo_id}")
        w.repos.update(repo_id=repo_id, branch="feature/firste2e_run")
        print("   ✅ Repo updated to latest commit")
    else:
        print("   ❌ Repo not found")
except Exception as e:
    print(f"   ❌ Error updating repo: {str(e)}")

# Step 2: Run the working framework pipeline
print("\n2. Running working framework pipeline...")

# Upload the validation notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/validate_complete_framework.py", "r") as f:
    validation_content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/validate_complete_framework",
    content=base64.b64encode(validation_content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Create validation job
validation_job = w.jobs.create(
    name=f"Complete Framework Validation - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="validate",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/validate_complete_framework"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID', '0721-134254-9s0ph6e7')
        )
    ]
)

validation_run = w.jobs.run_now(job_id=validation_job.job_id)
print(f"   ✅ Validation started: Run ID {validation_run.run_id}")
print(f"   📊 Monitor at: {os.getenv('DATABRICKS_HOST', 'https://adb-2908121449961741.1.azuredatabricks.net/')}#job/{validation_job.job_id}/run/{validation_run.run_id}")

# Wait for validation to complete
print("\n3. Waiting for validation to complete...")
start_time = time.time()
max_wait = 600  # 10 minutes

while time.time() - start_time < max_wait:
    run_info = w.jobs.get_run(run_id=validation_run.run_id)
    state = run_info.state
    
    if state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        print(f"\n   Status: {state.life_cycle_state}")
        
        if state.result_state == "SUCCESS":
            print("   ✅ Validation completed successfully!")
            
            # Get the output
            try:
                output = w.jobs.get_run_output(run_id=validation_run.run_id)
                if output.notebook_output and output.notebook_output.result:
                    result = output.notebook_output.result
                    print(f"   📊 Result: {result}")
                    
                    # Extract success rate
                    if "SUCCESS_RATE_" in result:
                        rate = int(result.split("_")[-1])
                        print(f"\n   🎯 SUCCESS RATE: {rate}%")
                        
                        if rate == 100:
                            print("   🎉 FRAMEWORK FULLY OPERATIONAL!")
                        elif rate >= 75:
                            print("   ✅ FRAMEWORK MOSTLY OPERATIONAL!")
                        else:
                            print("   ⚠️ FRAMEWORK NEEDS ATTENTION!")
            except Exception as e:
                print(f"   Error getting output: {str(e)}")
        else:
            print(f"   ❌ Validation failed: {state.result_state}")
            if state.state_message:
                print(f"   Message: {state.state_message}")
        break
    
    # Show progress
    elapsed = int(time.time() - start_time)
    if elapsed % 30 == 0:
        print(f"   ⏳ Still running... ({elapsed}s elapsed)")
    
    time.sleep(10)

# Clean up
try:
    w.jobs.delete(job_id=validation_job.job_id)
except:
    pass

print("\n✅ Update and validation complete!")
print(f"📊 Check Databricks workspace for detailed results: {os.getenv('DATABRICKS_HOST', 'https://adb-2908121449961741.1.azuredatabricks.net/')}")