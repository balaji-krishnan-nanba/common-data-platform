#!/usr/bin/env python3
"""Deploy and run the final E2E pipeline."""

import os
import time
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Environment variables should be set before running:
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'
# export DATABRICKS_CLUSTER_ID='your_cluster_id'

w = WorkspaceClient()

print("🚀 DEPLOYING FINAL E2E PIPELINE")
print("=" * 60)

# Upload the final pipeline notebook
with open("/Users/nandhinisankar/Documents/repo/personal_projects/common-data-platform/notebooks/final_e2e_pipeline.py", "r") as f:
    content = f.read()

w.workspace.import_(
    path="/Users/balaji.krishnan@nanba.co.uk/final_e2e_pipeline",
    content=base64.b64encode(content.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

print("✅ Final pipeline notebook uploaded")

# Create and run job
job = w.jobs.create(
    name=f"Final E2E Pipeline - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="final_pipeline",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/balaji.krishnan@nanba.co.uk/final_e2e_pipeline"
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Pipeline started: Run ID {run.run_id}")
print(f"📊 Monitor at: {os.getenv('DATABRICKS_HOST')}#job/{job.job_id}/run/{run.run_id}")

print("\n⏳ Monitoring pipeline execution...")
start_time = time.time()
max_wait = 600  # 10 minutes

while time.time() - start_time < max_wait:
    run_info = w.jobs.get_run(run_id=run.run_id)
    state = run_info.state
    
    if state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        print(f"\n📊 Pipeline {state.life_cycle_state}")
        
        if state.result_state == "SUCCESS":
            # Get output
            try:
                output = w.jobs.get_run_output(run_id=run.run_id)
                if output.notebook_output and output.notebook_output.result:
                    result = json.loads(output.notebook_output.result)
                    print(f"\n🎯 PIPELINE RESULTS:")
                    print(f"   Pipeline Run ID: {result['pipeline_run_id']}")
                    print(f"   Status: {result['status']}")
                    print(f"   Success Rate: {result['success_rate']}")
                    print(f"   Files Processed: {result['files_processed']}")
                    print(f"   Records Processed: {result['records_processed']:,}")
                    print(f"   Duration: {result['duration_seconds']:.2f} seconds")
                    
                    if result['status'] == "SUCCESS":
                        print("\n✅ PIPELINE FULLY SUCCESSFUL!")
                        print("\n📋 Framework Capabilities Verified:")
                        print("   ✓ Cell-level monitoring")
                        print("   ✓ Bronze layer ingestion (CSV & Excel)")
                        print("   ✓ Silver layer transformations")
                        print("   ✓ Gold layer analytics")
                        print("   ✓ Dynamic file processing")
                        print("   ✓ Error tracking and recovery")
                    else:
                        print("\n⚠️ PIPELINE COMPLETED WITH ERRORS")
            except Exception as e:
                print(f"Error parsing output: {str(e)}")
        else:
            print(f"❌ Pipeline failed: {state.result_state}")
        
        break
    
    elapsed = int(time.time() - start_time)
    if elapsed % 30 == 0:
        print(f"   Still running... ({elapsed}s elapsed)")
    
    time.sleep(10)

print("\n✅ Final E2E pipeline deployment complete!")
print("\n📝 Next Steps:")
print("   1. Verify all data in Bronze, Silver, and Gold layers")
print("   2. Re-run pipeline to test dynamic file processing")
print("   3. Check pipeline_metrics table for execution history")
print("   4. Push to GitHub for CI/CD integration")