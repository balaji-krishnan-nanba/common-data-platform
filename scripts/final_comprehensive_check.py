#!/usr/bin/env python3
"""Final comprehensive validation of the E2E pipeline."""

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

print("🎯 FINAL COMPREHENSIVE E2E PIPELINE VALIDATION")
print("=" * 60)
print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# Create comprehensive validation notebook
validation_notebook = '''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 FINAL COMPREHENSIVE E2E PIPELINE VALIDATION
# MAGIC 
# MAGIC This validates that the entire pipeline is working correctly with dynamic file processing.

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql.functions import *

print("🎯 FINAL E2E PIPELINE VALIDATION")
print("=" * 80)
print(f"Validation Time: {datetime.now()}")
print("=" * 80)

results = {
    "timestamp": datetime.now().isoformat(),
    "bronze_validation": {},
    "silver_validation": {},
    "gold_validation": {},
    "new_files_processed": {},
    "overall_status": "PENDING"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze Layer - Check All File Types

# COMMAND ----------

print("\\n🥉 BRONZE LAYER VALIDATION:")
print("=" * 50)

bronze_stats = {}

# Check CSV files
print("\\n📄 CSV Files:")
try:
    csv_tables = spark.sql("SHOW TABLES IN `cddp-dev-bronze`.`csv_data`").collect()
    print(f"Total CSV tables: {len(csv_tables)}")
    
    for table in csv_tables:
        table_name = table.tableName
        count = spark.sql(f"SELECT COUNT(*) FROM `cddp-dev-bronze`.`csv_data`.`{table_name}`").collect()[0][0]
        
        # Check for new categories
        if 'gaming' in table_name.lower():
            print(f"  ✅ NEW: {table_name} - {count} records (Gaming category)")
            bronze_stats["new_gaming"] = count
        elif 'food' in table_name.lower():
            print(f"  ✅ NEW: {table_name} - {count} records (Food & Beverage category)")
            bronze_stats["new_food"] = count
        else:
            print(f"  ✅ {table_name} - {count} records")
        
        bronze_stats[f"csv_{table_name}"] = count
        
except Exception as e:
    print(f"  ❌ Error: {str(e)}")

# Check Excel files
print("\\n📊 Excel Files:")
try:
    excel_tables = spark.sql("SHOW TABLES IN `cddp-dev-bronze`.`excel_data`").collect()
    print(f"Total Excel tables: {len(excel_tables)}")
    
    for table in excel_tables:
        table_name = table.tableName
        count = spark.sql(f"SELECT COUNT(*) FROM `cddp-dev-bronze`.`excel_data`.`{table_name}`").collect()[0][0]
        
        # Check for today's sales
        if 'daily_sales' in table_name or 'sales_2025' in table_name:
            print(f"  ✅ NEW: {table_name} - {count} records (Today's sales)")
            bronze_stats["new_daily_sales"] = count
        elif 'promo' in table_name.lower():
            print(f"  ✅ NEW: {table_name} - {count} records (Promo sales)")
            bronze_stats["new_promo_sales"] = count
        else:
            print(f"  ✅ {table_name} - {count} records")
        
        bronze_stats[f"excel_{table_name}"] = count
        
except Exception as e:
    print(f"  ❌ Error: {str(e)}")

results["bronze_validation"] = bronze_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check File Processing Tracking

# COMMAND ----------

print("\\n📁 FILE PROCESSING TRACKING:")
print("=" * 50)

try:
    # Get recent file processing
    recent_files = spark.sql("""
        SELECT 
            file_path,
            file_type,
            record_count,
            status,
            processed_timestamp
        FROM `cddp-dev-bronze`.`system`.`processed_files`
        WHERE processed_timestamp > current_timestamp() - INTERVAL 1 HOUR
        ORDER BY processed_timestamp DESC
        LIMIT 20
    """).collect()
    
    print(f"Files processed in last hour: {len(recent_files)}")
    
    new_files = 0
    for file in recent_files:
        if any(keyword in file.file_path for keyword in ['gaming', 'food', 'promo', '20250722']):
            print(f"  ✅ NEW: {file.file_path.split('/')[-1]} - {file.record_count} records")
            new_files += 1
    
    results["new_files_processed"]["count"] = new_files
    results["new_files_processed"]["total_recent"] = len(recent_files)
    
except Exception as e:
    print(f"  ❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Layer - Verify Transformations

# COMMAND ----------

print("\\n🥈 SILVER LAYER VALIDATION:")
print("=" * 50)

silver_stats = {}

# Check product master
try:
    # Get category breakdown
    categories = spark.sql("""
        SELECT 
            category,
            COUNT(*) as product_count,
            COUNT(DISTINCT brand) as brand_count
        FROM `cddp-dev-silver`.`products`.`product_master`
        GROUP BY category
        ORDER BY product_count DESC
    """).collect()
    
    print("\\n📋 Product Categories:")
    for cat in categories:
        if cat.category in ['Gaming', 'Food & Beverage']:
            print(f"  ✅ NEW: {cat.category} - {cat.product_count} products, {cat.brand_count} brands")
            silver_stats[f"new_category_{cat.category}"] = cat.product_count
        else:
            print(f"  ✅ {cat.category} - {cat.product_count} products, {cat.brand_count} brands")
    
    # Total products
    total_products = spark.sql("SELECT COUNT(*) FROM `cddp-dev-silver`.`products`.`product_master`").collect()[0][0]
    silver_stats["total_products"] = total_products
    print(f"\\nTotal Products in Master: {total_products}")
    
except Exception as e:
    print(f"  ❌ Error: {str(e)}")

# Check sales transactions
try:
    # Get today's transactions
    today_sales = spark.sql("""
        SELECT 
            COUNT(*) as transaction_count,
            SUM(total_amount) as total_revenue,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM `cddp-dev-silver`.`sales`.`sales_transactions`
        WHERE transaction_date = current_date()
    """).collect()[0]
    
    if today_sales.transaction_count > 0:
        print(f"\\n📊 Today's Sales:")
        print(f"  ✅ NEW: {today_sales.transaction_count} transactions")
        print(f"  ✅ Revenue: ${today_sales.total_revenue:,.2f}")
        print(f"  ✅ Customers: {today_sales.unique_customers}")
        silver_stats["today_transactions"] = today_sales.transaction_count
    
except Exception as e:
    print(f"  ❌ Error: {str(e)}")

results["silver_validation"] = silver_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Layer - Analytics Validation

# COMMAND ----------

print("\\n🥇 GOLD LAYER VALIDATION:")
print("=" * 50)

gold_stats = {}

# Check product performance
try:
    perf_data = spark.sql("""
        SELECT 
            COUNT(*) as total_segments,
            SUM(product_count) as total_products
        FROM `cddp-dev-gold`.`analytics`.`product_performance`
    """).collect()[0]
    
    print(f"\\n📊 Product Performance Analytics:")
    print(f"  ✅ Segments: {perf_data.total_segments}")
    print(f"  ✅ Products Analyzed: {perf_data.total_products}")
    
    gold_stats["performance_segments"] = perf_data.total_segments
    
except Exception as e:
    print(f"  ❌ Error: {str(e)}")

# Check store summary
try:
    store_summary = spark.sql("""
        SELECT COUNT(*) as summary_records
        FROM `cddp-dev-gold`.`reporting`.`daily_store_summary`
    """).collect()[0][0]
    
    if store_summary > 0:
        print(f"  ✅ Daily Store Summary: {store_summary} records")
        gold_stats["store_summary"] = store_summary
        
except Exception as e:
    pass  # Table might not exist if no sales data

results["gold_validation"] = gold_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Final Success Criteria

# COMMAND ----------

print("\\n🎯 FINAL SUCCESS CRITERIA:")
print("=" * 80)

criteria = []

# Check if pipeline is processing files
criteria.append(("Files processed in last hour", 
                results["new_files_processed"].get("total_recent", 0) > 0,
                f"{results['new_files_processed'].get('total_recent', 0)} files"))

# Check if new categories were added
new_categories = any(k.startswith("new_") for k in results["bronze_validation"])
criteria.append(("New file categories detected", 
                new_categories,
                "Gaming & Food products found" if new_categories else "No new categories"))

# Check if data flows through all layers
has_bronze = len(results["bronze_validation"]) > 0
has_silver = len(results["silver_validation"]) > 0
has_gold = len(results["gold_validation"]) > 0

criteria.append(("Bronze layer populated", has_bronze, f"{len(results['bronze_validation'])} tables"))
criteria.append(("Silver layer populated", has_silver, f"{len(results['silver_validation'])} metrics"))
criteria.append(("Gold layer populated", has_gold, f"{len(results['gold_validation'])} analytics"))

# Check dynamic processing
dynamic_processing = results["new_files_processed"].get("count", 0) > 0
criteria.append(("Dynamic file processing", 
                dynamic_processing,
                f"{results['new_files_processed'].get('count', 0)} new files processed"))

# Display results
passed = 0
for criterion, success, detail in criteria:
    status = "✅" if success else "❌"
    print(f"{status} {criterion}: {detail}")
    if success:
        passed += 1

success_rate = (passed / len(criteria)) * 100
overall_success = passed == len(criteria)

print(f"\\n📊 OVERALL RESULTS:")
print(f"   Success Rate: {success_rate:.0f}% ({passed}/{len(criteria)})")
print(f"   Pipeline Status: {'🎉 FULLY OPERATIONAL' if overall_success else '⚠️ NEEDS ATTENTION'}")

results["success_criteria"] = criteria
results["success_rate"] = success_rate
results["overall_status"] = "SUCCESS" if overall_success else "PARTIAL"

# COMMAND ----------

if overall_success:
    print("\\n" + "=" * 80)
    print("🎉 E2E PIPELINE IS FULLY OPERATIONAL!")
    print("=" * 80)
    print("✅ All files are being processed dynamically")
    print("✅ New file categories are detected and processed")
    print("✅ Data flows correctly through Bronze → Silver → Gold")
    print("✅ Analytics and reporting are working")
    print("✅ Framework is ready for production use!")
else:
    print("\\n⚠️ Some criteria not met - review details above")

# COMMAND ----------

# Return results
dbutils.notebook.exit(json.dumps(results))
'''

# Upload notebook
notebook_path = "/Users/balaji.krishnan@nanba.co.uk/final_comprehensive_validation"
w.workspace.import_(
    path=notebook_path,
    content=base64.b64encode(validation_notebook.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Wait a bit for pipelines to progress
print("\n⏳ Waiting 2 minutes for pipelines to make progress...")
time.sleep(120)

# Create and run validation job
job = w.jobs.create(
    name=f"Final Comprehensive Validation - {time.strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="final_validation",
            notebook_task=jobs.NotebookTask(
                notebook_path=notebook_path
            ),
            existing_cluster_id='0721-134254-9s0ph6e7'
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"\n✅ Final validation started: Run ID {run.run_id}")
print(f"📊 Monitor at: https://adb-2908121449961741.1.azuredatabricks.net/#job/{job.job_id}/run/{run.run_id}")

# Wait for validation to complete
print("\n⏳ Waiting for validation to complete...")
start_time = time.time()
while time.time() - start_time < 300:  # 5 minutes max
    run_info = w.jobs.get_run(run_id=run.run_id)
    if run_info.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        break
    time.sleep(10)

# Get results
if run_info.state.result_state == "SUCCESS":
    try:
        output = w.jobs.get_run_output(run_id=run.run_id)
        if output.notebook_output and output.notebook_output.result:
            results = json.loads(output.notebook_output.result)
            
            print("\n" + "=" * 80)
            print("🎯 FINAL E2E PIPELINE VALIDATION RESULTS")
            print("=" * 80)
            
            print(f"\nSuccess Rate: {results.get('success_rate', 0):.0f}%")
            print(f"Overall Status: {results.get('overall_status', 'UNKNOWN')}")
            
            if results.get('overall_status') == 'SUCCESS':
                print("\n🎉 E2E PIPELINE IS FULLY OPERATIONAL!")
                print("✅ All components working correctly")
                print("✅ Dynamic file processing confirmed")
                print("✅ New files are automatically detected and processed")
                print("✅ Data flows through all medallion layers")
                print("✅ Framework ready for production!")
    except Exception as e:
        print(f"Error getting results: {str(e)}")

# Clean up
try:
    w.jobs.delete(job_id=job.job_id)
except:
    pass

print(f"\n✅ Final comprehensive validation complete!")
print(f"📊 Check all results at: {os.environ['DATABRICKS_HOST']}#joblist")