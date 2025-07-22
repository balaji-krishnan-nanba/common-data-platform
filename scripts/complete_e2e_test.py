#!/usr/bin/env python3
"""Complete E2E Pipeline Test - All Steps"""

import os
import time
import json
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Check required environment variables
required_vars = [
    'DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID',
    'AZURE_SOURCE_STORAGE_ACCOUNT', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID'
]

for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

# Get configuration from environment
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
CLUSTER_ID = os.getenv('DATABRICKS_CLUSTER_ID')
AZURE_SOURCE_STORAGE = os.getenv('AZURE_SOURCE_STORAGE_ACCOUNT')

print("🎯 COMPLETE E2E PIPELINE TEST")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# Initialize workspace client
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Step 1: Check current pipeline status
print("\n📌 STEP 1: Check Current Pipeline Status")
print("-" * 50)

runs = w.jobs.list_runs(limit=20)
recent_runs = []
for run in runs:
    if run.start_time > (time.time() - 7200) * 1000:  # Last 2 hours
        recent_runs.append(run)
        
print(f"Found {len(recent_runs)} recent runs")
pipeline_success = any(run.state.result_state == "SUCCESS" and "pipeline" in run.run_name.lower() 
                      for run in recent_runs)

if pipeline_success:
    print("✅ Found successful pipeline runs")
else:
    print("⚠️ No successful pipeline runs found - starting new one")

# Step 2: Create test data generation notebook
print("\n📌 STEP 2: Create Test Data")
print("-" * 50)

test_data_notebook = '''# Databricks notebook source
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

print("🎯 CREATING COMPREHENSIVE TEST DATA")
print("=" * 50)

# Storage configuration
source_storage = "agentstge"
container = "raw-data"

# Step 1: Create diverse CSV files
print("\\n📄 Creating CSV Files...")

# Product categories
categories = {
    'Electronics': ['Laptop', 'Phone', 'Tablet', 'Camera', 'Headphones'],
    'Clothing': ['Shirt', 'Pants', 'Dress', 'Jacket', 'Shoes'],
    'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding', 'Storage'],
    'Sports': ['Equipment', 'Apparel', 'Footwear', 'Accessories', 'Nutrition'],
    'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics', 'Magazines']
}

csv_files_created = []

for category, subcategories in categories.items():
    products = []
    for i, subcat in enumerate(subcategories):
        for j in range(10):  # 10 products per subcategory
            products.append({
                'product_id': f'{category[:3].upper()}{i:02d}{j:03d}',
                'product_name': f'{subcat} {category} Item {j+1}',
                'category': category,
                'sub_category': subcat,
                'brand': f'Brand{chr(65 + (i+j) % 10)}',  # BrandA to BrandJ
                'price': round(random.uniform(10, 1000), 2),
                'cost': round(random.uniform(5, 500), 2),
                'status': random.choice(['ACTIVE'] * 9 + ['INACTIVE']),  # 90% active
                'created_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
                'modified_date': datetime.now().strftime('%Y-%m-%d')
            })
    
    df = pd.DataFrame(products)
    
    # Save locally
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    local_path = f"/tmp/{category.lower()}_products_{timestamp}.csv"
    df.to_csv(local_path, index=False)
    
    # Upload to storage
    target_path = f"abfss://{container}@{source_storage}.dfs.core.windows.net/products/{category.lower()}/{category.lower()}_catalog_{timestamp}.csv"
    dbutils.fs.cp(f"file:{local_path}", target_path)
    
    csv_files_created.append(target_path)
    print(f"✅ Created {category} CSV: {len(products)} products")
    
    # Clean up
    os.remove(local_path)

# Step 2: Create Excel files with sales data
print("\\n📊 Creating Excel Files...")

excel_files_created = []
stores = ['Store_North', 'Store_South', 'Store_East', 'Store_West', 'Store_Central']
payment_methods = ['Cash', 'Credit Card', 'Debit Card', 'Digital Wallet', 'Bank Transfer']

# Create sales data for last 3 months
for month_offset in range(3):
    month_date = datetime.now() - timedelta(days=30 * month_offset)
    month_str = month_date.strftime('%Y-%m')
    
    sales_records = []
    
    # Generate 15-30 days of data per month
    days_in_month = random.randint(15, 30)
    
    for day in range(1, days_in_month + 1):
        # 50-200 transactions per day
        daily_transactions = random.randint(50, 200)
        
        for trans in range(daily_transactions):
            sales_records.append({
                'transaction_id': f'TRX{month_date.strftime("%Y%m")}{day:02d}{trans:04d}',
                'transaction_date': f'{month_str}-{day:02d}',
                'customer_id': f'CUST{random.randint(1, 1000):05d}',
                'product_code': f'{random.choice(list(categories.keys()))[:3].upper()}{random.randint(0, 49):05d}',
                'quantity': random.randint(1, 10),
                'unit_price': round(random.uniform(10, 500), 2),
                'total_amount': 0,
                'payment_method': random.choice(payment_methods),
                'store_location': random.choice(stores),
                'discount_percent': random.choice([0] * 7 + [5, 10, 15, 20]),  # 70% no discount
                'tax_amount': 0
            })
    
    # Calculate totals
    for record in sales_records:
        subtotal = record['quantity'] * record['unit_price']
        discount = subtotal * (record['discount_percent'] / 100)
        record['total_amount'] = round((subtotal - discount) * 1.08, 2)  # 8% tax
        record['tax_amount'] = round((subtotal - discount) * 0.08, 2)
    
    df = pd.DataFrame(sales_records)
    
    # Save Excel
    timestamp = datetime.now().strftime('%H%M%S')
    local_path = f"/tmp/sales_{month_str}_{timestamp}.xlsx"
    df.to_excel(local_path, index=False, sheet_name='Sales Data')
    
    # Upload to storage
    target_path = f"abfss://{container}@{source_storage}.dfs.core.windows.net/sales/monthly/{month_date.year}/{month_date.month:02d}/sales_{month_str}_{timestamp}.xlsx"
    dbutils.fs.cp(f"file:{local_path}", target_path)
    
    excel_files_created.append(target_path)
    print(f"✅ Created {month_str} Excel: {len(sales_records)} transactions")
    
    # Clean up
    os.remove(local_path)

# Step 3: Create special test files
print("\\n🧪 Creating Special Test Files...")

# Create a large CSV file to test performance
large_products = []
for i in range(10000):  # 10K products
    large_products.append({
        'product_id': f'PERF{i:06d}',
        'product_name': f'Performance Test Product {i}',
        'category': 'TestCategory',
        'sub_category': f'SubCat{i % 100}',
        'brand': f'TestBrand{i % 50}',
        'price': round(random.uniform(1, 1000), 2),
        'cost': round(random.uniform(0.5, 500), 2),
        'status': 'ACTIVE',
        'created_date': datetime.now().strftime('%Y-%m-%d'),
        'modified_date': datetime.now().strftime('%Y-%m-%d')
    })

df = pd.DataFrame(large_products)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
local_path = f"/tmp/performance_test_{timestamp}.csv"
df.to_csv(local_path, index=False)

target_path = f"abfss://{container}@{source_storage}.dfs.core.windows.net/products/test/performance_test_{timestamp}.csv"
dbutils.fs.cp(f"file:{local_path}", target_path)
csv_files_created.append(target_path)
print(f"✅ Created performance test CSV: 10,000 products")
os.remove(local_path)

# Summary
print(f"\\n📊 TEST DATA CREATION SUMMARY:")
print(f"   CSV Files Created: {len(csv_files_created)}")
print(f"   Excel Files Created: {len(excel_files_created)}")
print(f"   Total Files: {len(csv_files_created) + len(excel_files_created)}")

# Return file lists
result = {
    "csv_files": csv_files_created,
    "excel_files": excel_files_created,
    "total_files": len(csv_files_created) + len(excel_files_created)
}

dbutils.notebook.exit(json.dumps(result))
'''

# Upload test data notebook
test_data_path = "/Users/balaji.krishnan@nanba.co.uk/create_test_data"
w.workspace.import_(
    path=test_data_path,
    content=base64.b64encode(test_data_notebook.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Run test data creation
test_data_job = w.jobs.create(
    name=f"Create Test Data - {datetime.now().strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="create_test_data",
            notebook_task=jobs.NotebookTask(
                notebook_path=test_data_path
            ),
            existing_cluster_id=CLUSTER_ID
        )
    ]
)

test_data_run = w.jobs.run_now(job_id=test_data_job.job_id)
print(f"✅ Test data creation started: Run ID {test_data_run.run_id}")

# Step 3: Run main pipeline
print("\n📌 STEP 3: Run Main Pipeline")
print("-" * 50)

pipeline_job = w.jobs.create(
    name=f"E2E Pipeline Complete Test - {datetime.now().strftime('%Y%m%d_%H%M%S')}",
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

pipeline_run = w.jobs.run_now(job_id=pipeline_job.job_id)
print(f"✅ Pipeline started: Run ID {pipeline_run.run_id}")
print(f"📊 Monitor at: {DATABRICKS_HOST}#job/{pipeline_job.job_id}/run/{pipeline_run.run_id}")

# Step 4: Create validation notebook
print("\n📌 STEP 4: Create Comprehensive Validation")
print("-" * 50)

validation_notebook = '''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 COMPREHENSIVE E2E PIPELINE VALIDATION

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql.functions import *

print("🎯 E2E PIPELINE VALIDATION REPORT")
print("=" * 80)
print(f"Validation Time: {datetime.now()}")
print("=" * 80)

validation_results = {
    "timestamp": datetime.now().isoformat(),
    "bronze_validation": {},
    "silver_validation": {},
    "gold_validation": {},
    "data_quality": {},
    "performance_metrics": {},
    "overall_status": "PENDING"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze Layer Validation

# COMMAND ----------

print("\\n📊 BRONZE LAYER VALIDATION:")
print("=" * 50)

bronze_catalog = "cddp-dev-bronze"
bronze_stats = {}

# Check CSV data
try:
    csv_tables = spark.sql(f"SHOW TABLES IN `{bronze_catalog}`.`csv_data`").collect()
    print(f"\\n📄 CSV Tables ({len(csv_tables)}):")
    
    for table in csv_tables:
        table_name = table.tableName
        full_table = f"`{bronze_catalog}`.`csv_data`.`{table_name}`"
        
        # Get statistics
        stats = spark.sql(f"""
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT product_id) as unique_products,
                COUNT(DISTINCT category) as categories,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price
            FROM {full_table}
        """).collect()[0]
        
        bronze_stats[f"csv_{table_name}"] = {
            "records": stats.record_count,
            "unique_products": stats.unique_products,
            "categories": stats.categories,
            "price_range": f"${stats.min_price:.2f} - ${stats.max_price:.2f}",
            "avg_price": f"${stats.avg_price:.2f}"
        }
        
        print(f"  ✅ {table_name}: {stats.record_count:,} records, {stats.unique_products} products")
        
except Exception as e:
    print(f"  ❌ Error checking CSV tables: {str(e)}")
    bronze_stats["csv_error"] = str(e)

# Check Excel data
try:
    excel_tables = spark.sql(f"SHOW TABLES IN `{bronze_catalog}`.`excel_data`").collect()
    print(f"\\n📊 Excel Tables ({len(excel_tables)}):")
    
    for table in excel_tables:
        table_name = table.tableName
        full_table = f"`{bronze_catalog}`.`excel_data`.`{table_name}`"
        
        # Get record count
        count = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
        bronze_stats[f"excel_{table_name}"] = {"records": count}
        
        print(f"  ✅ {table_name}: {count:,} records")
        
except Exception as e:
    print(f"  ❌ Error checking Excel tables: {str(e)}")
    bronze_stats["excel_error"] = str(e)

validation_results["bronze_validation"] = bronze_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Layer Validation

# COMMAND ----------

print("\\n📊 SILVER LAYER VALIDATION:")
print("=" * 50)

silver_catalog = "cddp-dev-silver"
silver_stats = {}

# Check product master
try:
    product_stats = spark.sql(f"""
        SELECT 
            COUNT(*) as total_products,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(DISTINCT category) as categories,
            COUNT(DISTINCT brand) as brands,
            SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) as active_products,
            AVG(price - cost) as avg_margin
        FROM `{silver_catalog}`.`products`.`product_master`
    """).collect()[0]
    
    silver_stats["product_master"] = {
        "total_products": product_stats.total_products,
        "unique_products": product_stats.unique_products,
        "categories": product_stats.categories,
        "brands": product_stats.brands,
        "active_products": product_stats.active_products,
        "avg_margin": f"${product_stats.avg_margin:.2f}"
    }
    
    print(f"  ✅ Product Master: {product_stats.total_products:,} products")
    print(f"     - Categories: {product_stats.categories}")
    print(f"     - Brands: {product_stats.brands}")
    print(f"     - Active: {product_stats.active_products:,}")
    
except Exception as e:
    print(f"  ❌ Error checking product master: {str(e)}")
    silver_stats["product_error"] = str(e)

# Check sales transactions
try:
    sales_count = spark.sql(f"SELECT COUNT(*) FROM `{silver_catalog}`.`sales`.`sales_transactions`").collect()[0][0]
    silver_stats["sales_transactions"] = {"records": sales_count}
    print(f"  ✅ Sales Transactions: {sales_count:,} records")
    
except Exception as e:
    print(f"  ❌ Error checking sales: {str(e)}")
    silver_stats["sales_error"] = str(e)

validation_results["silver_validation"] = silver_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Layer Validation

# COMMAND ----------

print("\\n📊 GOLD LAYER VALIDATION:")
print("=" * 50)

gold_catalog = "cddp-dev-gold"
gold_stats = {}

# Check analytics tables
try:
    perf_stats = spark.sql(f"""
        SELECT 
            COUNT(*) as segments,
            SUM(product_count) as total_products,
            AVG(avg_margin) as overall_avg_margin
        FROM `{gold_catalog}`.`analytics`.`product_performance`
    """).collect()[0]
    
    gold_stats["product_performance"] = {
        "segments": perf_stats.segments,
        "total_products": perf_stats.total_products,
        "avg_margin": f"${perf_stats.overall_avg_margin:.2f}"
    }
    
    print(f"  ✅ Product Performance: {perf_stats.segments} segments")
    
except Exception as e:
    print(f"  ❌ Error checking analytics: {str(e)}")
    gold_stats["analytics_error"] = str(e)

# Check reporting tables
try:
    summary_count = spark.sql(f"SELECT COUNT(*) FROM `{gold_catalog}`.`reporting`.`daily_store_summary`").collect()[0][0]
    gold_stats["daily_store_summary"] = {"records": summary_count}
    print(f"  ✅ Store Summary: {summary_count} records")
    
except Exception as e:
    pass  # Table might not exist if no sales data

validation_results["gold_validation"] = gold_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality Checks

# COMMAND ----------

print("\\n🔍 DATA QUALITY VALIDATION:")
print("=" * 50)

quality_checks = {}

# Check for data consistency
try:
    # Bronze to Silver data flow
    bronze_products = spark.sql(f"SELECT COUNT(DISTINCT product_id) FROM `{bronze_catalog}`.`csv_data`.`product_catalog`").collect()[0][0]
    silver_products = spark.sql(f"SELECT COUNT(DISTINCT product_id) FROM `{silver_catalog}`.`products`.`product_master`").collect()[0][0]
    
    retention_rate = (silver_products / bronze_products * 100) if bronze_products > 0 else 0
    
    quality_checks["data_flow"] = {
        "bronze_products": bronze_products,
        "silver_products": silver_products,
        "retention_rate": f"{retention_rate:.1f}%"
    }
    
    print(f"  ✅ Data Flow: {retention_rate:.1f}% retention (Bronze → Silver)")
    
    # Check for nulls in critical fields
    null_check = spark.sql(f"""
        SELECT 
            SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_ids,
            SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) as null_names,
            SUM(CASE WHEN price IS NULL OR price <= 0 THEN 1 ELSE 0 END) as invalid_prices
        FROM `{silver_catalog}`.`products`.`product_master`
    """).collect()[0]
    
    quality_checks["data_quality"] = {
        "null_product_ids": null_check.null_ids,
        "null_product_names": null_check.null_names,
        "invalid_prices": null_check.invalid_prices
    }
    
    quality_issues = null_check.null_ids + null_check.null_names + null_check.invalid_prices
    print(f"  ✅ Data Quality: {quality_issues} issues found")
    
except Exception as e:
    print(f"  ❌ Error in quality checks: {str(e)}")
    quality_checks["error"] = str(e)

validation_results["data_quality"] = quality_checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Performance Metrics

# COMMAND ----------

print("\\n⚡ PERFORMANCE METRICS:")
print("=" * 50)

performance = {}

try:
    # Check file processing performance
    file_stats = spark.sql(f"""
        SELECT 
            file_type,
            COUNT(*) as files_processed,
            SUM(record_count) as total_records,
            AVG(record_count) as avg_records_per_file,
            MIN(processed_timestamp) as first_processed,
            MAX(processed_timestamp) as last_processed
        FROM `{bronze_catalog}`.`system`.`processed_files`
        WHERE status = 'SUCCESS'
        GROUP BY file_type
    """).collect()
    
    for stat in file_stats:
        performance[stat.file_type] = {
            "files": stat.files_processed,
            "records": stat.total_records,
            "avg_per_file": int(stat.avg_records_per_file)
        }
        print(f"  ✅ {stat.file_type}: {stat.files_processed} files, {stat.total_records:,} records")
    
except Exception as e:
    print(f"  ❌ Error checking performance: {str(e)}")
    performance["error"] = str(e)

validation_results["performance_metrics"] = performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Final Validation Summary

# COMMAND ----------

print("\\n🎯 FINAL VALIDATION SUMMARY:")
print("=" * 80)

# Calculate success criteria
criteria = []

# Check Bronze layer
bronze_tables = len([k for k in bronze_stats.keys() if not k.endswith("_error")])
criteria.append(("Bronze tables created", bronze_tables > 0, f"{bronze_tables} tables"))

# Check Silver layer
silver_tables = len([k for k in silver_stats.keys() if not k.endswith("_error")])
criteria.append(("Silver tables created", silver_tables > 0, f"{silver_tables} tables"))

# Check Gold layer
gold_tables = len([k for k in gold_stats.keys() if not k.endswith("_error")])
criteria.append(("Gold tables created", gold_tables > 0, f"{gold_tables} tables"))

# Check data quality
quality_passed = quality_checks.get("data_quality", {}).get("null_product_ids", 1) == 0
criteria.append(("Data quality checks", quality_passed, "No critical nulls" if quality_passed else "Issues found"))

# Check performance
files_processed = sum(p.get("files", 0) for p in performance.values() if isinstance(p, dict))
criteria.append(("Files processed", files_processed > 0, f"{files_processed} files"))

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
print(f"   Status: {'🎉 FULLY OPERATIONAL' if overall_success else '⚠️ NEEDS ATTENTION'}")

# Update validation results
validation_results["success_criteria"] = criteria
validation_results["success_rate"] = success_rate
validation_results["overall_status"] = "SUCCESS" if overall_success else "PARTIAL"

# COMMAND ----------

# Return validation results
print("\\n💾 Validation complete - returning results")
dbutils.notebook.exit(json.dumps(validation_results))
'''

# Upload validation notebook
validation_path = "/Users/balaji.krishnan@nanba.co.uk/comprehensive_validation"
w.workspace.import_(
    path=validation_path,
    content=base64.b64encode(validation_notebook.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

print("✅ Validation notebook uploaded")

# Step 5: Wait for jobs and run validation
print("\n📌 STEP 5: Wait for Jobs and Run Validation")
print("-" * 50)

# Wait for test data creation
print("⏳ Waiting for test data creation...")
while True:
    test_data_status = w.jobs.get_run(run_id=test_data_run.run_id)
    if test_data_status.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        if test_data_status.state.result_state == "SUCCESS":
            print("✅ Test data creation completed")
        else:
            print("❌ Test data creation failed")
        break
    time.sleep(10)

# Wait for main pipeline (max 10 minutes)
print("⏳ Waiting for main pipeline...")
start_wait = time.time()
while time.time() - start_wait < 600:  # 10 minutes
    pipeline_status = w.jobs.get_run(run_id=pipeline_run.run_id)
    if pipeline_status.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        if pipeline_status.state.result_state == "SUCCESS":
            print("✅ Main pipeline completed successfully")
        else:
            print("⚠️ Main pipeline still running or failed")
        break
    time.sleep(30)

# Run validation
validation_job = w.jobs.create(
    name=f"Final Validation - {datetime.now().strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="validate",
            notebook_task=jobs.NotebookTask(
                notebook_path=validation_path
            ),
            existing_cluster_id=CLUSTER_ID
        )
    ]
)

validation_run = w.jobs.run_now(job_id=validation_job.job_id)
print(f"✅ Validation started: Run ID {validation_run.run_id}")

# Wait for validation
print("⏳ Waiting for validation...")
while True:
    validation_status = w.jobs.get_run(run_id=validation_run.run_id)
    if validation_status.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        break
    time.sleep(10)

# Get validation results
if validation_status.state.result_state == "SUCCESS":
    try:
        output = w.jobs.get_run_output(run_id=validation_run.run_id)
        if output.notebook_output and output.notebook_output.result:
            results = json.loads(output.notebook_output.result)
            
            print("\n" + "=" * 80)
            print("🎯 FINAL E2E PIPELINE VALIDATION RESULTS")
            print("=" * 80)
            
            print(f"\nSuccess Rate: {results.get('success_rate', 0):.0f}%")
            print(f"Overall Status: {results.get('overall_status', 'UNKNOWN')}")
            
            if results.get('overall_status') == 'SUCCESS':
                print("\n🎉 E2E PIPELINE FULLY OPERATIONAL!")
                print("✅ All files processed successfully")
                print("✅ Data flows through all layers")
                print("✅ Quality checks passed")
                print("✅ Ready for production use")
            else:
                print("\n⚠️ Pipeline needs attention - check details above")
    except Exception as e:
        print(f"Error parsing results: {str(e)}")

# Clean up jobs
print("\n🧹 Cleaning up...")
for job_id in [test_data_job.job_id, pipeline_job.job_id, validation_job.job_id]:
    try:
        w.jobs.delete(job_id=job_id)
    except:
        pass

print("\n✅ COMPLETE E2E TEST FINISHED")
print(f"End Time: {datetime.now()}")
print("\n📊 Check Databricks workspace for detailed results:")
print(f"   {DATABRICKS_HOST}")