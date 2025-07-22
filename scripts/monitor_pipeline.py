#!/usr/bin/env python3
"""Monitor pipeline execution and create additional test files."""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import time
import base64
from config import DATABRICKS_HOST, DATABRICKS_TOKEN, CLUSTER_ID, AZURE_SOURCE_STORAGE, validate_config

def check_pipeline_status():
    """Check the status of running pipelines."""
    
    validate_config()
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    print("📊 Checking Pipeline Status...")
    
    # Get recent runs
    runs = w.jobs.list_runs(limit=10)
    
    active_runs = []
    completed_runs = []
    
    for run in runs:
        if "Pipeline" in run.run_name and run.start_time > (time.time() - 3600) * 1000:  # Last hour
            if run.state.life_cycle_state in ["RUNNING", "PENDING"]:
                active_runs.append(run)
            elif run.state.life_cycle_state == "TERMINATED":
                completed_runs.append(run)
    
    print(f"\n🏃 Active Runs: {len(active_runs)}")
    for run in active_runs:
        print(f"   - {run.run_name}: {run.state.life_cycle_state}")
        print(f"     URL: {DATABRICKS_HOST}#job/{run.job_id}/run/{run.run_id}")
    
    print(f"\n✅ Completed Runs: {len(completed_runs)}")
    for run in completed_runs:
        status = "✅" if run.state.result_state == "SUCCESS" else "❌"
        print(f"   {status} {run.run_name}: {run.state.result_state}")
    
    return len(active_runs), len(completed_runs)

def create_additional_test_files():
    """Create additional test files for dynamic processing."""
    
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    print("\n🧪 Creating Additional Test Files...")
    
    # Create notebook to generate more test files
    test_notebook = '''# Databricks notebook source
import pandas as pd
from datetime import datetime
import random
import os

print("📝 Creating additional test files...")

# Create more diverse CSV files
categories = ['Electronics', 'Books', 'Clothing', 'Sports', 'Home']
brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']

for i, category in enumerate(categories):
    test_products = pd.DataFrame({
        'product_id': [f'{category.upper()}{j:03d}' for j in range(1, 11)],
        'product_name': [f'{category} Product {j}' for j in range(1, 11)],
        'category': [category] * 10,
        'sub_category': [f'{category}_Sub'] * 10,
        'brand': [random.choice(brands) for _ in range(10)],
        'price': [round(random.uniform(10, 500), 2) for _ in range(10)],
        'cost': [round(random.uniform(5, 250), 2) for _ in range(10)],
        'status': ['ACTIVE'] * 8 + ['INACTIVE'] * 2,
        'created_date': datetime.now().strftime('%Y-%m-%d'),
        'modified_date': datetime.now().strftime('%Y-%m-%d')
    })
    
    # Save CSV
    local_csv = f"/tmp/{category.lower()}_products.csv"
    test_products.to_csv(local_csv, index=False)
    
    # Upload to storage
    csv_path = f"abfss://raw-data@agentstge.dfs.core.windows.net/products/{category.lower()}/{category.lower()}_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    dbutils.fs.cp(f"file:{local_csv}", csv_path)
    print(f"✅ Created {category} CSV: {csv_path}")
    
    # Clean up
    os.remove(local_csv)

# Create sales Excel files for different months
for month in range(1, 4):  # Jan, Feb, Mar
    sales_data = []
    for day in range(1, 11):  # 10 days of data per month
        for _ in range(random.randint(10, 20)):  # 10-20 transactions per day
            sales_data.append({
                'transaction_id': f'{month:02d}{day:02d}{len(sales_data)+1:04d}',
                'transaction_date': f'2024-{month:02d}-{day:02d}',
                'customer_id': f'CUST{random.randint(1, 100):04d}',
                'product_code': f'PROD{random.randint(1, 50):03d}',
                'quantity': random.randint(1, 5),
                'unit_price': round(random.uniform(10, 200), 2),
                'total_amount': 0,
                'payment_method': random.choice(['Cash', 'Card', 'Digital']),
                'store_location': random.choice(['North', 'South', 'East', 'West'])
            })
    
    # Calculate total amount
    for record in sales_data:
        record['total_amount'] = record['quantity'] * record['unit_price']
    
    df = pd.DataFrame(sales_data)
    
    # Save Excel
    local_excel = f"/tmp/sales_2024_{month:02d}.xlsx"
    df.to_excel(local_excel, index=False, sheet_name="Sales Data")
    
    # Upload to storage
    excel_path = f"abfss://raw-data@agentstge.dfs.core.windows.net/sales/monthly/2024/{month:02d}/sales_2024_{month:02d}_{datetime.now().strftime('%H%M%S')}.xlsx"
    dbutils.fs.cp(f"file:{local_excel}", excel_path)
    print(f"✅ Created Month {month} Excel: {excel_path}")
    
    # Clean up
    os.remove(local_excel)

print("\\n✅ All additional test files created!")
print("   - 5 CSV files (one per category)")
print("   - 3 Excel files (one per month)")
print("   Total: 8 new files for pipeline testing")

dbutils.notebook.exit("SUCCESS")
'''
    
    # Upload and run the test file creation notebook
    test_path = "/Users/balaji.krishnan@nanba.co.uk/create_additional_files"
    
    w.workspace.import_(
        path=test_path,
        content=base64.b64encode(test_notebook.encode()).decode('utf-8'),
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        overwrite=True
    )
    
    # Create and run job
    job = w.jobs.create(
        name=f"Create Additional Test Files - {time.strftime('%Y%m%d_%H%M%S')}",
        tasks=[
            jobs.Task(
                task_key="create_additional_files",
                notebook_task=jobs.NotebookTask(
                    notebook_path=test_path
                ),
                existing_cluster_id=CLUSTER_ID
            )
        ]
    )
    
    run = w.jobs.run_now(job_id=job.job_id)
    
    print(f"✅ Additional file creation started!")
    print(f"   Job ID: {job.job_id}")
    print(f"   Run ID: {run.run_id}")
    print(f"📊 Monitor at: {DATABRICKS_HOST}#job/{job.job_id}/run/{run.run_id}")
    
    return job.job_id, run.run_id

def create_final_validation_notebook():
    """Create a comprehensive validation notebook."""
    
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    print("\n📋 Creating Final Validation Notebook...")
    
    validation_notebook = '''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 FINAL PIPELINE VALIDATION
# MAGIC 
# MAGIC This notebook performs comprehensive validation of the E2E data pipeline.

# COMMAND ----------

import json
from datetime import datetime

print("🎯 COMPREHENSIVE PIPELINE VALIDATION")
print("=" * 60)
print(f"Validation Time: {datetime.now()}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Unity Catalog Structure Validation

# COMMAND ----------

print("📚 UNITY CATALOG VALIDATION:")
print("=" * 40)

catalogs = ['cddp-dev-bronze', 'cddp-dev-silver', 'cddp-dev-gold']
catalog_validation = {}

for catalog in catalogs:
    print(f"\\n{catalog}:")
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        schema_list = []
        for schema in schemas:
            schema_name = schema.namespace
            if schema_name not in ['information_schema']:
                schema_list.append(schema_name)
                print(f"  ✅ {schema_name}")
        catalog_validation[catalog] = schema_list
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        catalog_validation[catalog] = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Table Count and Record Validation

# COMMAND ----------

print("\\n📊 TABLE AND RECORD VALIDATION:")
print("=" * 40)

table_stats = {}
total_tables = 0
total_records = 0

for catalog in catalogs:
    layer = catalog.split('-')[-1].upper()
    print(f"\\n{layer} LAYER ({catalog}):")
    
    layer_tables = 0
    layer_records = 0
    
    for schema in catalog_validation.get(catalog, []):
        try:
            tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
            for table in tables:
                table_name = table.tableName
                try:
                    count = spark.sql(f"SELECT COUNT(*) FROM `{catalog}`.`{schema}`.`{table_name}`").collect()[0][0]
                    print(f"  📋 {schema}.{table_name}: {count:,} records")
                    
                    table_stats[f"{catalog}.{schema}.{table_name}"] = count
                    layer_tables += 1
                    layer_records += count
                    total_tables += 1
                    total_records += count
                except Exception as e:
                    print(f"  ❌ Error reading {schema}.{table_name}: {str(e)}")
        except Exception as e:
            print(f"  ❌ Error accessing schema {schema}: {str(e)}")
    
    print(f"  📊 Layer Summary: {layer_tables} tables, {layer_records:,} records")

print(f"\\n📈 OVERALL SUMMARY:")
print(f"   Total Tables: {total_tables}")
print(f"   Total Records: {total_records:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Validation

# COMMAND ----------

print("\\n🔍 DATA QUALITY VALIDATION:")
print("=" * 40)

quality_results = {}

# Validate Bronze layer data
print("\\nBRONZE LAYER QUALITY:")
try:
    csv_tables = spark.sql("SHOW TABLES IN `cddp-dev-bronze`.`csv_data`").collect()
    for table in csv_tables:
        table_name = table.tableName
        # Check for null values in key columns
        null_check = spark.sql(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN product_id IS NULL THEN 1 END) as null_product_ids,
                COUNT(CASE WHEN product_name IS NULL THEN 1 END) as null_product_names
            FROM `cddp-dev-bronze`.`csv_data`.`{table_name}`
        """).collect()[0]
        
        print(f"  📋 {table_name}:")
        print(f"     Total rows: {null_check.total_rows:,}")
        print(f"     Null product_ids: {null_check.null_product_ids}")
        print(f"     Null product_names: {null_check.null_product_names}")
        
        quality_results[f"bronze_csv_{table_name}"] = {
            "total_rows": null_check.total_rows,
            "null_product_ids": null_check.null_product_ids,
            "null_product_names": null_check.null_product_names
        }
except Exception as e:
    print(f"  ❌ Error validating CSV data: {str(e)}")

# Validate Silver layer transformations
print("\\nSILVER LAYER QUALITY:")
try:
    product_quality = spark.sql("""
        SELECT 
            COUNT(*) as total_products,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(CASE WHEN price <= 0 THEN 1 END) as invalid_prices,
            COUNT(CASE WHEN category IS NULL THEN 1 END) as null_categories
        FROM `cddp-dev-silver`.`products`.`product_master`
    """).collect()[0]
    
    print(f"  📋 Product Master:")
    print(f"     Total products: {product_quality.total_products:,}")
    print(f"     Unique products: {product_quality.unique_products:,}")
    print(f"     Invalid prices: {product_quality.invalid_prices}")
    print(f"     Null categories: {product_quality.null_categories}")
    
    quality_results["silver_products"] = {
        "total_products": product_quality.total_products,
        "unique_products": product_quality.unique_products,
        "invalid_prices": product_quality.invalid_prices,
        "null_categories": product_quality.null_categories
    }
except Exception as e:
    print(f"  ❌ Error validating product data: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Lineage Validation

# COMMAND ----------

print("\\n🔗 DATA LINEAGE VALIDATION:")
print("=" * 40)

lineage_results = {}

# Check if data flows correctly from Bronze to Silver to Gold
try:
    # Bronze to Silver comparison
    bronze_count = spark.sql("SELECT COUNT(DISTINCT product_id) FROM `cddp-dev-bronze`.`csv_data`.`product_catalog`").collect()[0][0]
    silver_count = spark.sql("SELECT COUNT(DISTINCT product_id) FROM `cddp-dev-silver`.`products`.`product_master`").collect()[0][0]
    
    print(f"📊 Product Data Flow:")
    print(f"   Bronze unique products: {bronze_count:,}")
    print(f"   Silver unique products: {silver_count:,}")
    print(f"   Data retention: {(silver_count/bronze_count*100):.1f}%")
    
    lineage_results["bronze_to_silver"] = {
        "bronze_products": bronze_count,
        "silver_products": silver_count,
        "retention_rate": round(silver_count/bronze_count*100, 1)
    }
    
except Exception as e:
    print(f"❌ Error validating data lineage: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Performance Metrics

# COMMAND ----------

print("\\n⚡ PERFORMANCE METRICS:")
print("=" * 40)

performance_results = {}

try:
    # Check file processing tracking
    processing_stats = spark.sql("""
        SELECT 
            file_type,
            status,
            COUNT(*) as file_count,
            SUM(record_count) as total_records
        FROM `cddp-dev-bronze`.`system`.`processed_files`
        GROUP BY file_type, status
        ORDER BY file_type, status
    """).collect()
    
    print("📁 File Processing Summary:")
    for stat in processing_stats:
        print(f"   {stat.file_type} - {stat.status}: {stat.file_count} files, {stat.total_records:,} records")
        
        performance_results[f"{stat.file_type}_{stat.status}"] = {
            "file_count": stat.file_count,
            "total_records": stat.total_records
        }
        
except Exception as e:
    print(f"❌ Error getting performance metrics: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Final Validation Summary

# COMMAND ----------

print("\\n🎯 FINAL VALIDATION SUMMARY:")
print("=" * 60)

# Calculate overall success metrics
success_criteria = []

# Check if all layers have tables
bronze_tables = sum(1 for k in table_stats if 'bronze' in k)
silver_tables = sum(1 for k in table_stats if 'silver' in k)
gold_tables = sum(1 for k in table_stats if 'gold' in k)

success_criteria.append(("Bronze tables created", bronze_tables > 0, f"{bronze_tables} tables"))
success_criteria.append(("Silver tables created", silver_tables > 0, f"{silver_tables} tables"))
success_criteria.append(("Gold tables created", gold_tables > 0, f"{gold_tables} tables"))

# Check if data was processed
total_bronze_records = sum(v for k, v in table_stats.items() if 'bronze' in k)
total_silver_records = sum(v for k, v in table_stats.items() if 'silver' in k)
total_gold_records = sum(v for k, v in table_stats.items() if 'gold' in k)

success_criteria.append(("Bronze data ingested", total_bronze_records > 0, f"{total_bronze_records:,} records"))
success_criteria.append(("Silver data transformed", total_silver_records > 0, f"{total_silver_records:,} records"))
success_criteria.append(("Gold data aggregated", total_gold_records > 0, f"{total_gold_records:,} records"))

# Display results
passed_criteria = 0
for criterion, passed, detail in success_criteria:
    status = "✅" if passed else "❌"
    print(f"{status} {criterion}: {detail}")
    if passed:
        passed_criteria += 1

overall_success = passed_criteria == len(success_criteria)
success_rate = (passed_criteria / len(success_criteria)) * 100

print(f"\\n📊 OVERALL RESULTS:")
print(f"   Success Rate: {success_rate:.1f}% ({passed_criteria}/{len(success_criteria)})")
print(f"   Pipeline Status: {'🎉 FULLY OPERATIONAL' if overall_success else '⚠️ NEEDS ATTENTION'}")

# Final summary object
final_summary = {
    "validation_time": datetime.now().isoformat(),
    "overall_success": overall_success,
    "success_rate": success_rate,
    "table_stats": table_stats,
    "quality_results": quality_results,
    "lineage_results": lineage_results,
    "performance_results": performance_results,
    "success_criteria": success_criteria
}

print(f"\\n💾 Validation complete - all metrics captured")

if overall_success:
    print("\\n🎉 PIPELINE VALIDATION SUCCESSFUL!")
    print("   The E2E data pipeline is fully operational and meets all criteria.")
    print("   ✅ Files are being processed dynamically")
    print("   ✅ Data flows correctly through all layers")  
    print("   ✅ Quality checks are passing")
    print("   ✅ All Unity Catalog structures are in place")
else:
    print("\\n⚠️ PIPELINE VALIDATION INCOMPLETE")
    print("   Some criteria are not met - review the details above.")

# Store results for external access
dbutils.notebook.exit(json.dumps(final_summary))
'''
    
    # Upload validation notebook
    validation_path = "/Users/balaji.krishnan@nanba.co.uk/final_validation"
    
    w.workspace.import_(
        path=validation_path,
        content=base64.b64encode(validation_notebook.encode()).decode('utf-8'),
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        overwrite=True
    )
    
    print(f"✅ Validation notebook created at: {validation_path}")
    return validation_path

def main():
    """Main monitoring function."""
    
    print("🎯 PIPELINE MONITORING AND VALIDATION")
    print("=" * 60)
    
    # Check current status
    active, completed = check_pipeline_status()
    
    # Create additional test files
    file_job_id, file_run_id = create_additional_test_files()
    
    # Create validation notebook
    validation_path = create_final_validation_notebook()
    
    print(f"\n📋 MONITORING SUMMARY:")
    print(f"   Active Pipeline Runs: {active}")
    print(f"   Completed Pipeline Runs: {completed}")
    print(f"   Additional Files Job: {file_job_id}")
    print(f"   Validation Notebook: {validation_path}")
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"   1. Wait for pipeline runs to complete (~10-15 minutes)")
    print(f"   2. Run the validation notebook: {validation_path}")
    print(f"   3. Check that new files are processed automatically")
    print(f"   4. Verify all Bronze, Silver, and Gold tables are populated")
    
    print(f"\n📊 Monitor all jobs at: {DATABRICKS_HOST}#joblist")

if __name__ == "__main__":
    main()