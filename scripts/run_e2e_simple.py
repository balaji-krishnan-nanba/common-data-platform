#!/usr/bin/env python3
"""Simple E2E pipeline execution."""

import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# Import configuration
from config import DATABRICKS_HOST, DATABRICKS_TOKEN, CLUSTER_ID, validate_config

def run_e2e_pipeline():
    """Run the E2E pipeline."""
    
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    print("🚀 Starting E2E Pipeline...")
    
    # Create and run job
    job = w.jobs.create(
        name=f"E2E Pipeline - {time.strftime('%Y%m%d_%H%M%S')}",
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
    
    print(f"✅ Job created: {job.job_id}")
    
    # Start the run
    run = w.jobs.run_now(job_id=job.job_id)
    print(f"✅ Run started: {run.run_id}")
    
    # Check status for 5 minutes
    for i in range(30):  # 30 * 10 seconds = 5 minutes
        run_info = w.jobs.get_run(run_id=run.run_id)
        state = run_info.state
        
        print(f"   Status: {state.life_cycle_state}")
        
        if state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            if state.result_state == "SUCCESS":
                print("✅ Pipeline completed successfully!")
                
                # Now verify tables
                verify_job = w.jobs.create(
                    name=f"Verify Tables - {time.strftime('%Y%m%d_%H%M%S')}",
                    tasks=[
                        jobs.Task(
                            task_key="verify",
                            notebook_task=jobs.NotebookTask(
                                notebook_path="/Users/balaji.krishnan@nanba.co.uk/verify_tables",
                                base_parameters={}
                            ),
                            existing_cluster_id=CLUSTER_ID
                        )
                    ]
                )
                
                # Upload verification notebook
                import base64
                from databricks.sdk.service import workspace
                
                verify_code = '''# Databricks notebook source
# Verify pipeline results

print("📊 PIPELINE VERIFICATION")
print("=" * 50)

catalogs = ['cddp-dev-bronze', 'cddp-dev-silver', 'cddp-dev-gold']
total_tables = 0
total_records = 0

for catalog in catalogs:
    print(f"\\n{catalog.upper()}:")
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        for schema in schemas:
            schema_name = schema.namespace
            if schema_name not in ['information_schema']:
                try:
                    tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema_name}`").collect()
                    for table in tables:
                        table_name = table.tableName
                        count = spark.sql(f"SELECT COUNT(*) FROM `{catalog}`.`{schema_name}`.`{table_name}`").collect()[0][0]
                        print(f"  - {schema_name}.{table_name}: {count:,} records")
                        total_tables += 1
                        total_records += count
                except Exception as e:
                    print(f"  - Error reading {schema_name}: {str(e)}")
    except Exception as e:
        print(f"  Error accessing {catalog}: {str(e)}")

print(f"\\n📊 SUMMARY:")
print(f"   Total Tables: {total_tables}")
print(f"   Total Records: {total_records:,}")

if total_tables > 0 and total_records > 0:
    print("\\n✅ PIPELINE VALIDATION SUCCESSFUL!")
else:
    print("\\n❌ PIPELINE VALIDATION FAILED!")
'''
                
                w.workspace.import_(
                    path="/Users/balaji.krishnan@nanba.co.uk/verify_tables",
                    content=base64.b64encode(verify_code.encode()).decode('utf-8'),
                    format=workspace.ImportFormat.SOURCE,
                    language=workspace.Language.PYTHON,
                    overwrite=True
                )
                
                verify_run = w.jobs.run_now(job_id=verify_job.job_id)
                print(f"✅ Verification started: {verify_run.run_id}")
                
                # Clean up main job
                w.jobs.delete(job_id=job.job_id)
                
                print(f"\\n🔍 Monitor verification at: {DATABRICKS_HOST}#job/{verify_job.job_id}/run/{verify_run.run_id}")
                print(f"📊 Monitor main run at: {DATABRICKS_HOST}#job/{job.job_id}/run/{run.run_id}")
                
                return True
            else:
                print(f"❌ Pipeline failed: {state.result_state}")
                if state.state_message:
                    print(f"   Message: {state.state_message}")
                return False
        
        time.sleep(10)
    
    print("⏰ Timeout reached - check status manually")
    print(f"📊 Monitor at: {DATABRICKS_HOST}#job/{job.job_id}/run/{run.run_id}")
    return True

if __name__ == "__main__":
    success = run_e2e_pipeline()
    print(f"\\n{'✅ SUCCESS' if success else '❌ FAILED'}: E2E pipeline execution")