#!/usr/bin/env python3
"""Deploy and run cell-level monitoring pipeline."""

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

print("🚀 DEPLOYING CELL-LEVEL MONITORING PIPELINE")
print("=" * 60)

# Upload the cell monitoring notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/cell_monitor_pipeline.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/cell_monitor_pipeline",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

print("✅ Cell monitoring notebook uploaded")

# Create and run job
job = w.jobs.create(
    name=f"Cell Monitor Pipeline - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="cell_monitor",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/cell_monitor_pipeline"
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
max_wait = 600  # 10 minutes

while time.time() - start_time < max_wait:
    run_info = w.jobs.get_run(run_id=run.run_id)
    state = run_info.state
    
    if state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        print(f"\n📊 Pipeline {state.life_cycle_state}")
        
        # Get output
        try:
            output = w.jobs.get_run_output(run_id=run.run_id)
            if output.notebook_output and output.notebook_output.result:
                import json
                result = json.loads(output.notebook_output.result)
                print(f"\n📈 CELL EXECUTION RESULTS:")
                print(f"   Status: {result['status']}")
                print(f"   Total Cells: {result['total_cells']}")
                print(f"   Successful: {result['successful_cells']}")
                print(f"   Failed: {result['failed_cells']}")
                print(f"   Success Rate: {result['success_rate']}")
                print(f"   Duration: {result['duration_seconds']:.2f} seconds")
                
                if result['critical_failures'] > 0:
                    print(f"   ⚠️ Critical Failures: {result['critical_failures']}")
        except Exception as e:
            print(f"Error parsing output: {str(e)}")
        
        break
    
    elapsed = int(time.time() - start_time)
    if elapsed % 30 == 0:
        print(f"   Still running... ({elapsed}s elapsed)")
    
    time.sleep(10)

print("\n✅ Cell monitoring pipeline complete!")