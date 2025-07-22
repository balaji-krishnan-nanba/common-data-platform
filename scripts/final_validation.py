#!/usr/bin/env python3
"""Run final validation of the E2E pipeline."""

import time
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from config import DATABRICKS_HOST, DATABRICKS_TOKEN, CLUSTER_ID, validate_config

def run_final_validation():
    """Run the final validation notebook."""
    
    validate_config()
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    print("🎯 RUNNING FINAL VALIDATION")
    print("=" * 50)
    
    # Create and run validation job
    job = w.jobs.create(
        name=f"Final Validation - {time.strftime('%Y%m%d_%H%M%S')}",
        tasks=[
            jobs.Task(
                task_key="final_validation",
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Users/balaji.krishnan@nanba.co.uk/final_validation"
                ),
                existing_cluster_id=CLUSTER_ID
            )
        ]
    )
    
    print(f"📋 Created validation job: {job.job_id}")
    
    # Start the run
    run = w.jobs.run_now(job_id=job.job_id)
    print(f"🏃 Started validation run: {run.run_id}")
    print(f"📊 Monitor at: {DATABRICKS_HOST}#job/{job.job_id}/run/{run.run_id}")
    
    # Wait for completion
    print("⏳ Waiting for validation to complete...")
    
    for i in range(60):  # 10 minutes max
        run_info = w.jobs.get_run(run_id=run.run_id)
        state = run_info.state
        
        if state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
        
        if i % 6 == 0:  # Print every minute
            print(f"   Status: {state.life_cycle_state} ({i//6} min elapsed)")
        
        time.sleep(10)
    
    # Get final results
    final_state = run_info.state
    
    if final_state.result_state == "SUCCESS":
        print("✅ Validation completed successfully!")
        
        # Get the validation results
        try:
            output = w.jobs.get_run_output(run_id=run.run_id)
            if output.notebook_output and output.notebook_output.result:
                validation_data = json.loads(output.notebook_output.result)
                
                print("\n📊 VALIDATION RESULTS:")
                print("=" * 50)
                
                overall_success = validation_data.get('overall_success', False)
                success_rate = validation_data.get('success_rate', 0)
                
                print(f"Overall Success: {'✅ YES' if overall_success else '❌ NO'}")
                print(f"Success Rate: {success_rate:.1f}%")
                
                # Show table statistics
                table_stats = validation_data.get('table_stats', {})
                if table_stats:
                    print(f"\n📋 TABLE STATISTICS:")
                    bronze_tables = [k for k in table_stats if 'bronze' in k]
                    silver_tables = [k for k in table_stats if 'silver' in k]
                    gold_tables = [k for k in table_stats if 'gold' in k]
                    
                    print(f"   Bronze Tables: {len(bronze_tables)}")
                    print(f"   Silver Tables: {len(silver_tables)}")
                    print(f"   Gold Tables: {len(gold_tables)}")
                    print(f"   Total Records: {sum(table_stats.values()):,}")
                
                # Show quality results
                quality_results = validation_data.get('quality_results', {})
                if quality_results:
                    print(f"\n🔍 DATA QUALITY:")
                    for key, value in quality_results.items():
                        if isinstance(value, dict):
                            print(f"   {key}:")
                            for k, v in value.items():
                                print(f"     - {k}: {v}")
                
                if overall_success:
                    print(f"\n🎉 PIPELINE FULLY OPERATIONAL!")
                    print(f"   ✅ All files processed successfully")
                    print(f"   ✅ Data flows through all layers")
                    print(f"   ✅ Quality checks passing")
                    print(f"   ✅ Dynamic processing working")
                else:
                    print(f"\n⚠️ PIPELINE NEEDS ATTENTION!")
                    print(f"   Some validation criteria failed")
                
                # Clean up
                w.jobs.delete(job_id=job.job_id)
                
                return overall_success
                
        except Exception as e:
            print(f"⚠️ Could not parse validation results: {str(e)}")
    
    else:
        print(f"❌ Validation failed: {final_state.result_state}")
        if final_state.state_message:
            print(f"   Error: {final_state.state_message}")
    
    return False

def check_all_jobs():
    """Check status of all recent jobs."""
    
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    print("\n📊 CHECKING ALL RECENT JOBS:")
    print("=" * 50)
    
    # Get recent runs
    runs = w.jobs.list_runs(limit=20)
    
    recent_runs = []
    for run in runs:
        if run.start_time > (time.time() - 7200) * 1000:  # Last 2 hours
            recent_runs.append(run)
    
    pipeline_runs = []
    test_runs = []
    validation_runs = []
    
    for run in recent_runs:
        run_name = run.run_name.lower()
        if 'pipeline' in run_name and 'test' in run_name:
            pipeline_runs.append(run)
        elif 'additional' in run_name or 'create' in run_name:
            test_runs.append(run)
        elif 'validation' in run_name:
            validation_runs.append(run)
    
    print(f"Pipeline Runs: {len(pipeline_runs)}")
    for run in pipeline_runs[:3]:  # Show latest 3
        status = "✅" if run.state.result_state == "SUCCESS" else "❌" if run.state.result_state == "FAILED" else "🔄"
        print(f"  {status} {run.run_name}: {run.state.result_state}")
    
    print(f"\nTest File Creation: {len(test_runs)}")
    for run in test_runs[:3]:
        status = "✅" if run.state.result_state == "SUCCESS" else "❌" if run.state.result_state == "FAILED" else "🔄"
        print(f"  {status} {run.run_name}: {run.state.result_state}")
    
    print(f"\nValidation Runs: {len(validation_runs)}")
    for run in validation_runs[:3]:
        status = "✅" if run.state.result_state == "SUCCESS" else "❌" if run.state.result_state == "FAILED" else "🔄"
        print(f"  {status} {run.run_name}: {run.state.result_state}")
    
    successful_pipelines = sum(1 for run in pipeline_runs if run.state.result_state == "SUCCESS")
    successful_tests = sum(1 for run in test_runs if run.state.result_state == "SUCCESS")
    
    return successful_pipelines, successful_tests

def main():
    """Main validation function."""
    
    print("🎯 FINAL E2E PIPELINE VALIDATION")
    print("=" * 60)
    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Check current job status
    successful_pipelines, successful_tests = check_all_jobs()
    
    # Run final validation
    validation_success = run_final_validation()
    
    print(f"\n🎯 FINAL SUMMARY:")
    print("=" * 60)
    print(f"✅ Successful Pipeline Runs: {successful_pipelines}")
    print(f"✅ Successful Test File Creations: {successful_tests}")
    print(f"✅ Final Validation: {'PASSED' if validation_success else 'FAILED'}")
    
    overall_success = (
        successful_pipelines > 0 and 
        successful_tests > 0 and 
        validation_success
    )
    
    if overall_success:
        print(f"\n🎉 COMPLETE SUCCESS!")
        print(f"   The E2E pipeline is fully operational")
        print(f"   All validation criteria met")
        print(f"   Dynamic file processing confirmed")
        print(f"   Ready for production use")
    else:
        print(f"\n⚠️ PARTIAL SUCCESS")
        print(f"   Some components may need attention")
        print(f"   Review individual job results")
    
    print(f"\n📊 Access Databricks workspace:")
    print(f"   {DATABRICKS_HOST}")
    
    return overall_success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)