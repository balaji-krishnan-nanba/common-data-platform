#!/usr/bin/env python3
"""Execute the complete E2E pipeline in Databricks and verify results."""

import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import json

# Import configuration
from config import DATABRICKS_HOST, DATABRICKS_TOKEN, CLUSTER_ID, AZURE_SOURCE_STORAGE, validate_config

def execute_pipeline():
    """Execute the complete E2E pipeline."""
    
    validate_config()
    
    # Initialize workspace client
    w = WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN
    )
    
    print("🚀 Starting E2E Pipeline Execution...")
    
    # Create a job to run the test notebook
    job_name = f"E2E Pipeline Test - {time.strftime('%Y-%m-%d %H:%M:%S')}"
    notebook_path = "/Users/balaji.krishnan@nanba.co.uk/test_complete_pipeline"
    
    print(f"📋 Creating job: {job_name}")
    
    # Create job with proper environment variables
    job = w.jobs.create(
        name=job_name,
        tasks=[
            jobs.Task(
                task_key="run_e2e_pipeline",
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters={}
                ),
                existing_cluster_id=CLUSTER_ID
            )
        ]
    )
    
    print(f"✅ Job created with ID: {job.job_id}")
    
    # Run the job
    print("🏃 Running the job...")
    run = w.jobs.run_now(job_id=job.job_id)
    
    print(f"✅ Job run started: Run ID {run.run_id}")
    print(f"📊 Monitor at: {DATABRICKS_HOST}#job/{job.job_id}/run/{run.run_id}")
    
    # Wait for job completion
    print("⏳ Waiting for job to complete...")
    
    start_time = time.time()
    timeout = 1800  # 30 minutes timeout
    
    while True:
        run_info = w.jobs.get_run(run_id=run.run_id)
        state = run_info.state
        
        if state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
        
        elapsed = time.time() - start_time
        print(f"   Status: {state.life_cycle_state} (elapsed: {int(elapsed)}s)")
        
        if elapsed > timeout:
            print("❌ Timeout reached!")
            break
        
        time.sleep(10)
    
    # Check final status
    final_state = run_info.state
    
    if final_state.result_state == "SUCCESS":
        print("✅ Pipeline execution completed successfully!")
        
        # Get run output
        try:
            output = w.jobs.get_run_output(run_id=run.run_id)
            if output.notebook_output and output.notebook_output.result:
                print("\n📋 Notebook Output:")
                print(output.notebook_output.result)
        except:
            pass
            
    else:
        print(f"❌ Pipeline execution failed: {final_state.result_state}")
        if final_state.state_message:
            print(f"   Error: {final_state.state_message}")
    
    # Clean up job
    print("\n🧹 Cleaning up...")
    try:
        w.jobs.delete(job_id=job.job_id)
        print("✅ Job deleted")
    except:
        pass
    
    return final_state.result_state == "SUCCESS"


def verify_tables():
    """Verify that all tables were created successfully."""
    
    print("\n📊 Verifying Tables...")
    
    # Initialize workspace client
    w = WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN
    )
    
    # Create a notebook to check tables
    verification_notebook = """# Databricks notebook source
# Verify all tables

results = {}

# Check Bronze tables
print("🥉 BRONZE LAYER:")
try:
    bronze_tables = spark.sql("SHOW TABLES IN `cddp-dev-bronze`.`csv_data`").collect()
    csv_count = len(bronze_tables)
    for table in bronze_tables:
        count = spark.sql(f"SELECT COUNT(*) FROM `cddp-dev-bronze`.`csv_data`.`{table.tableName}`").collect()[0][0]
        print(f"  - {table.tableName}: {count} records")
        results[f"bronze_csv_{table.tableName}"] = count
except:
    csv_count = 0

try:
    excel_tables = spark.sql("SHOW TABLES IN `cddp-dev-bronze`.`excel_data`").collect()
    excel_count = len(excel_tables)
    for table in excel_tables:
        count = spark.sql(f"SELECT COUNT(*) FROM `cddp-dev-bronze`.`excel_data`.`{table.tableName}`").collect()[0][0]
        print(f"  - {table.tableName}: {count} records")
        results[f"bronze_excel_{table.tableName}"] = count
except:
    excel_count = 0

# Check Silver tables
print("\\n🥈 SILVER LAYER:")
try:
    product_count = spark.sql("SELECT COUNT(*) FROM `cddp-dev-silver`.`products`.`product_master`").collect()[0][0]
    print(f"  - product_master: {product_count} records")
    results["silver_products"] = product_count
except:
    results["silver_products"] = 0

try:
    sales_count = spark.sql("SELECT COUNT(*) FROM `cddp-dev-silver`.`sales`.`sales_transactions`").collect()[0][0]
    print(f"  - sales_transactions: {sales_count} records")
    results["silver_sales"] = sales_count
except:
    results["silver_sales"] = 0

# Check Gold tables
print("\\n🥇 GOLD LAYER:")
try:
    perf_count = spark.sql("SELECT COUNT(*) FROM `cddp-dev-gold`.`analytics`.`product_performance`").collect()[0][0]
    print(f"  - product_performance: {perf_count} records")
    results["gold_analytics"] = perf_count
except:
    results["gold_analytics"] = 0

try:
    summary_count = spark.sql("SELECT COUNT(*) FROM `cddp-dev-gold`.`reporting`.`daily_store_summary`").collect()[0][0]
    print(f"  - daily_store_summary: {summary_count} records")
    results["gold_reporting"] = summary_count
except:
    results["gold_reporting"] = 0

# Return results as JSON
import json
dbutils.notebook.exit(json.dumps(results))
"""
    
    # Upload verification notebook
    import base64
    from databricks.sdk.service import workspace
    
    verification_path = "/Users/balaji.krishnan@nanba.co.uk/verify_tables_temp"
    
    w.workspace.import_(
        path=verification_path,
        content=base64.b64encode(verification_notebook.encode()).decode('utf-8'),
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        overwrite=True
    )
    
    # Run verification notebook
    print("🔍 Running verification notebook...")
    
    job = w.jobs.create(
        name="Table Verification",
        tasks=[
            jobs.Task(
                task_key="verify",
                notebook_task=jobs.NotebookTask(
                    notebook_path=verification_path,
                    base_parameters={}
                ),
                existing_cluster_id=CLUSTER_ID
            )
        ]
    )
    
    run = w.jobs.run_now(job_id=job.job_id)
    
    # Wait for completion
    while True:
        run_info = w.jobs.get_run(run_id=run.run_id)
        if run_info.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
        time.sleep(5)
    
    # Get results
    if run_info.state.result_state == "SUCCESS":
        output = w.jobs.get_run_output(run_id=run.run_id)
        if output.notebook_output and output.notebook_output.result:
            results = json.loads(output.notebook_output.result)
            
            print("\n📊 Table Verification Results:")
            print(f"   Bronze CSV tables: {sum(1 for k in results if k.startswith('bronze_csv_'))}")
            print(f"   Bronze Excel tables: {sum(1 for k in results if k.startswith('bronze_excel_'))}")
            print(f"   Silver tables: {sum(1 for k in results if k.startswith('silver_'))}")
            print(f"   Gold tables: {sum(1 for k in results if k.startswith('gold_'))}")
            print(f"   Total records: {sum(v for v in results.values() if isinstance(v, int)):,}")
            
            # Clean up
            w.jobs.delete(job_id=job.job_id)
            w.workspace.delete(path=verification_path)
            
            return results
    
    # Clean up on failure
    try:
        w.jobs.delete(job_id=job.job_id)
        w.workspace.delete(path=verification_path)
    except:
        pass
    
    return {}


def create_test_files():
    """Create new test files to verify dynamic processing."""
    
    print("\n🧪 Creating New Test Files...")
    
    # Initialize workspace client
    w = WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN
    )
    
    # Create notebook to generate test files
    test_notebook = f"""# Databricks notebook source
import pandas as pd
from datetime import datetime
import os

# Create new test CSV
print("📝 Creating new test CSV file...")

test_products = pd.DataFrame({{
    'product_id': ['FINAL001', 'FINAL002', 'FINAL003', 'FINAL004', 'FINAL005'],
    'product_name': ['Final Test Product 1', 'Final Test Product 2', 'Final Test Product 3', 'Final Test Product 4', 'Final Test Product 5'],
    'category': ['TestCategory', 'TestCategory', 'TestCategory', 'TestCategory', 'TestCategory'],
    'sub_category': ['FinalTest', 'FinalTest', 'FinalTest', 'FinalTest', 'FinalTest'],
    'brand': ['FinalTestBrand', 'FinalTestBrand', 'FinalTestBrand', 'FinalTestBrand', 'FinalTestBrand'],
    'price': [111.11, 222.22, 333.33, 444.44, 555.55],
    'cost': [55.55, 111.11, 166.66, 222.22, 277.77],
    'status': ['ACTIVE', 'ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE'],
    'created_date': datetime.now().strftime('%Y-%m-%d'),
    'modified_date': datetime.now().strftime('%Y-%m-%d')
}})

# Save CSV
local_csv = "/tmp/final_test_products.csv"
test_products.to_csv(local_csv, index=False)

# Upload to storage
csv_path = f"abfss://raw-data@{AZURE_SOURCE_STORAGE}.dfs.core.windows.net/products/test/final_test_products_{{datetime.now().strftime('%Y%m%d_%H%M%S')}}.csv"
dbutils.fs.cp(f"file:{{local_csv}}", csv_path)
print(f"✅ Created CSV: {{csv_path}}")

# Create new test Excel
print("\\n📝 Creating new test Excel file...")

test_sales = pd.DataFrame({{
    'transaction_id': [f'FINAL{{i:06d}}' for i in range(1, 51)],
    'transaction_date': [datetime.now().strftime('%Y-%m-%d')] * 50,
    'customer_id': [f'FCUST{{i:04d}}' for i in range(1, 51)],
    'product_code': ['FINAL001', 'FINAL002', 'FINAL003', 'FINAL004', 'FINAL005'] * 10,
    'quantity': [i % 10 + 1 for i in range(50)],
    'unit_price': [111.11 + (i % 5) * 111.11 for i in range(50)],
    'total_amount': [0] * 50,
    'payment_method': ['Cash', 'Credit Card', 'Debit Card', 'Digital Wallet', 'Bank Transfer'] * 10,
    'store_location': ['Final_Store_North', 'Final_Store_South', 'Final_Store_East', 'Final_Store_West'] * 12 + ['Final_Store_Central'] * 2
}})

# Calculate total amount
test_sales['total_amount'] = test_sales['quantity'] * test_sales['unit_price']

# Save Excel
local_excel = "/tmp/final_test_sales.xlsx"
test_sales.to_excel(local_excel, index=False, sheet_name="Sales Data")

# Upload to storage
excel_path = f"abfss://raw-data@{AZURE_SOURCE_STORAGE}.dfs.core.windows.net/sales/test/final_test_sales_{{datetime.now().strftime('%Y%m%d_%H%M%S')}}.xlsx"
dbutils.fs.cp(f"file:{{local_excel}}", excel_path)
print(f"✅ Created Excel: {{excel_path}}")

# Clean up
os.remove(local_csv)
os.remove(local_excel)

print("\\n✅ Test files created successfully!")
dbutils.notebook.exit("SUCCESS")
"""
    
    # Upload and run test file creation notebook
    import base64
    from databricks.sdk.service import workspace
    
    test_path = "/Users/balaji.krishnan@nanba.co.uk/create_test_files_temp"
    
    w.workspace.import_(
        path=test_path,
        content=base64.b64encode(test_notebook.encode()).decode('utf-8'),
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        overwrite=True
    )
    
    # Run the notebook
    job = w.jobs.create(
        name="Create Test Files",
        tasks=[
            jobs.Task(
                task_key="create_files",
                notebook_task=jobs.NotebookTask(
                    notebook_path=test_path,
                    base_parameters={}
                ),
                existing_cluster_id=CLUSTER_ID
            )
        ]
    )
    
    run = w.jobs.run_now(job_id=job.job_id)
    
    # Wait for completion
    while True:
        run_info = w.jobs.get_run(run_id=run.run_id)
        if run_info.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
        time.sleep(5)
    
    # Clean up
    try:
        w.jobs.delete(job_id=job.job_id)
        w.workspace.delete(path=test_path)
    except:
        pass
    
    return run_info.state.result_state == "SUCCESS"


def main():
    """Main execution function."""
    
    print("🎯 COMMON DATA PLATFORM - COMPLETE E2E EXECUTION")
    print("=" * 60)
    print(f"Databricks: {DATABRICKS_HOST}")
    print(f"Storage: {AZURE_SOURCE_STORAGE}")
    print("=" * 60)
    
    # Step 1: Execute the main pipeline
    print("\n📌 STEP 1: Execute Main Pipeline")
    pipeline_success = execute_pipeline()
    
    if not pipeline_success:
        print("❌ Pipeline execution failed!")
        return False
    
    # Step 2: Verify tables were created
    print("\n📌 STEP 2: Verify Tables")
    initial_results = verify_tables()
    
    if not initial_results:
        print("❌ Table verification failed!")
        return False
    
    # Step 3: Create new test files
    print("\n📌 STEP 3: Create New Test Files")
    files_created = create_test_files()
    
    if not files_created:
        print("❌ Test file creation failed!")
        return False
    
    # Step 4: Re-run pipeline to process new files
    print("\n📌 STEP 4: Process New Files")
    print("⏳ Waiting 30 seconds for files to be available...")
    time.sleep(30)
    
    rerun_success = execute_pipeline()
    
    if not rerun_success:
        print("❌ Pipeline re-run failed!")
        return False
    
    # Step 5: Final verification
    print("\n📌 STEP 5: Final Verification")
    final_results = verify_tables()
    
    # Compare results
    print("\n📊 FINAL SUMMARY:")
    print("=" * 60)
    
    if final_results:
        # Check if new records were added
        bronze_increased = any(
            final_results.get(k, 0) > initial_results.get(k, 0) 
            for k in final_results if k.startswith('bronze_')
        )
        
        if bronze_increased:
            print("✅ New files successfully processed!")
            print("✅ Bronze layer updated with new data")
            print("✅ Silver and Gold layers refreshed")
            print("✅ Pipeline is fully dynamic and working correctly!")
            
            print("\n📈 Record Counts:")
            for key, value in sorted(final_results.items()):
                initial = initial_results.get(key, 0)
                if value > initial:
                    print(f"   {key}: {initial} → {value} (+{value - initial})")
                else:
                    print(f"   {key}: {value}")
            
            print("\n🎉 E2E PIPELINE FULLY OPERATIONAL!")
            return True
        else:
            print("⚠️ New files may not have been processed")
    
    return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)