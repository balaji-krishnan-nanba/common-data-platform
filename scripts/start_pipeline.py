#!/usr/bin/env python3
"""Start the E2E pipeline execution."""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import time
from config import DATABRICKS_HOST, DATABRICKS_TOKEN, CLUSTER_ID, validate_config

def start_pipeline():
    """Start the E2E pipeline."""
    
    validate_config()
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    print("🚀 Starting E2E Pipeline...")
    
    # Create job
    job = w.jobs.create(
        name=f"E2E Pipeline Final Test - {time.strftime('%Y%m%d_%H%M%S')}",
        tasks=[
            jobs.Task(
                task_key="e2e_pipeline",
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Users/balaji.krishnan@nanba.co.uk/test_complete_pipeline"
                ),
                existing_cluster_id=CLUSTER_ID
            )
        ]
    )
    
    # Start run
    run = w.jobs.run_now(job_id=job.job_id)
    
    print(f"✅ Pipeline started successfully!")
    print(f"   Job ID: {job.job_id}")
    print(f"   Run ID: {run.run_id}")
    print(f"📊 Monitor at: {DATABRICKS_HOST}#job/{job.job_id}/run/{run.run_id}")
    
    # Also start the simple e2e notebook
    job2 = w.jobs.create(
        name=f"E2E Simple Pipeline - {time.strftime('%Y%m%d_%H%M%S')}",
        tasks=[
            jobs.Task(
                task_key="e2e_simple",
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Users/balaji.krishnan@nanba.co.uk/e2e_pipeline_complete"
                ),
                existing_cluster_id=CLUSTER_ID
            )
        ]
    )
    
    run2 = w.jobs.run_now(job_id=job2.job_id)
    
    print(f"✅ Simple pipeline also started!")
    print(f"   Job ID: {job2.job_id}")
    print(f"   Run ID: {run2.run_id}")
    print(f"📊 Monitor at: {DATABRICKS_HOST}#job/{job2.job_id}/run/{run2.run_id}")
    
    print(f"\n🎯 Both pipelines are now running in parallel!")
    print(f"   Check the Databricks workspace for results")
    print(f"   Expected runtime: 10-15 minutes")

if __name__ == "__main__":
    start_pipeline()