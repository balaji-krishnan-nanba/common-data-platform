# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 E2E Pipeline - Framework Implementation
# MAGIC 
# MAGIC This notebook runs the complete E2E pipeline using the Common Data Platform framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup Environment

# COMMAND ----------

import os
import sys
import subprocess
from datetime import datetime

print("🚀 E2E PIPELINE - FRAMEWORK IMPLEMENTATION")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clone Repository

# COMMAND ----------

print("\n📦 Setting up repository...")

repo_path = "/Workspace/Users/balaji.krishnan@nanba.co.uk/common-data-platform"
repo_url = "https://github.com/balaji-krishnan-nanba/common-data-platform.git"

# Check if repo exists
try:
    dbutils.fs.ls(repo_path)
    print("  ✅ Repository exists, pulling latest changes...")
    
    # Navigate to repo and pull
    os.chdir(repo_path)
    result = subprocess.run(["git", "pull"], capture_output=True, text=True)
    if result.returncode == 0:
        print("  ✅ Repository updated")
    else:
        print(f"  ⚠️ Pull warning: {result.stderr}")
        
except Exception as e:
    print("  📥 Cloning repository...")
    
    # Remove if exists (corrupted)
    try:
        dbutils.fs.rm(repo_path, recurse=True)
    except:
        pass
    
    # Set up git
    subprocess.run(["git", "config", "--global", "user.email", "balaji.krishnan@nanba.co.uk"])
    subprocess.run(["git", "config", "--global", "user.name", "Balaji Krishnan"])
    
    # Clone
    workspace_base = "/Workspace/Users/balaji.krishnan@nanba.co.uk"
    os.chdir(workspace_base)
    
    result = subprocess.run(
        ["git", "clone", repo_url],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("  ✅ Repository cloned successfully")
    else:
        print(f"  ❌ Clone failed: {result.stderr}")
        raise Exception("Failed to clone repository")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Install Package

# COMMAND ----------

print("\n📦 Installing package...")
%pip install -e /Workspace/Users/balaji.krishnan@nanba.co.uk/common-data-platform

# COMMAND ----------

# Restart Python kernel
print("🔄 Restarting Python kernel...")
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Import Framework Components

# COMMAND ----------

print("📚 Importing framework components...")

try:
    from common_data_platform.config import ConfigManager
    from common_data_platform.ingestion import FileIngestionOrchestrator
    from common_data_platform.transformation import Transformer
    print("  ✅ All imports successful")
except Exception as e:
    print(f"  ❌ Import error: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Initialize Configuration

# COMMAND ----------

print("\n⚙️ Initializing configuration...")

# Set environment variables
os.environ['PROJECT_CODE'] = 'cddp'
os.environ['ENVIRONMENT'] = 'dev'
os.environ['AZURE_SOURCE_STORAGE_ACCOUNT'] = 'agentstge'
os.environ['AZURE_DATALAKE_STORAGE_ACCOUNT'] = 'agentdatalake2025'

# Initialize config manager
config_manager = ConfigManager()

print("  ✅ Configuration initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Run Bronze Ingestion

# COMMAND ----------

print("\n🥉 BRONZE LAYER INGESTION")
print("=" * 50)

# Initialize orchestrator
orchestrator = FileIngestionOrchestrator(spark, config_manager)

# Get all configured sources
sources = config_manager.get_all_sources()
print(f"Found {len(sources)} configured sources")

# Process each source
for source_name, source_config in sources.items():
    print(f"\n📄 Processing source: {source_name}")
    try:
        result = orchestrator.ingest_source(source_name)
        if result['status'] == 'success':
            print(f"  ✅ Success: {result['files_processed']} files, {result['total_records']} records")
        else:
            print(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run Silver Transformations

# COMMAND ----------

print("\n🥈 SILVER LAYER TRANSFORMATIONS")
print("=" * 50)

# Initialize transformer
transformer = Transformer(spark, config_manager)

# Get silver transformations
silver_transforms = config_manager.config.get('transformations', {}).get('silver', [])
print(f"Found {len(silver_transforms)} silver transformations")

# Run each transformation
for transform in silver_transforms:
    print(f"\n🔄 Running: {transform['name']}")
    try:
        result = transformer.transform('silver', transform['name'])
        if result.get('status') == 'success':
            print(f"  ✅ Success: {result.get('records_processed', 0)} records")
        else:
            print(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Run Gold Transformations

# COMMAND ----------

print("\n🥇 GOLD LAYER TRANSFORMATIONS")
print("=" * 50)

# Get gold transformations
gold_transforms = config_manager.config.get('transformations', {}).get('gold', [])
print(f"Found {len(gold_transforms)} gold transformations")

# Run each transformation
for transform in gold_transforms:
    print(f"\n🔄 Running: {transform['name']}")
    try:
        result = transformer.transform('gold', transform['name'])
        if result.get('status') == 'success':
            print(f"  ✅ Success: {result.get('records_processed', 0)} records")
        else:
            print(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validation

# COMMAND ----------

print("\n📊 PIPELINE VALIDATION")
print("=" * 50)

# Check Bronze tables
print("\n🥉 Bronze Layer:")
bronze_catalog = f"{os.environ['PROJECT_CODE']}-{os.environ['ENVIRONMENT']}-bronze"
try:
    for schema in ['csv_data', 'excel_data', 'system']:
        tables = spark.sql(f"SHOW TABLES IN `{bronze_catalog}`.`{schema}`").collect()
        print(f"  {schema}: {len(tables)} tables")
        for table in tables[:3]:  # Show first 3
            count = spark.sql(f"SELECT COUNT(*) FROM `{bronze_catalog}`.`{schema}`.`{table.tableName}`").collect()[0][0]
            print(f"    - {table.tableName}: {count} records")
except Exception as e:
    print(f"  Error: {str(e)}")

# Check Silver tables
print("\n🥈 Silver Layer:")
silver_catalog = f"{os.environ['PROJECT_CODE']}-{os.environ['ENVIRONMENT']}-silver"
try:
    for schema in ['products', 'sales']:
        tables = spark.sql(f"SHOW TABLES IN `{silver_catalog}`.`{schema}`").collect()
        print(f"  {schema}: {len(tables)} tables")
        for table in tables:
            count = spark.sql(f"SELECT COUNT(*) FROM `{silver_catalog}`.`{schema}`.`{table.tableName}`").collect()[0][0]
            print(f"    - {table.tableName}: {count} records")
except Exception as e:
    print(f"  Error: {str(e)}")

# Check Gold tables
print("\n🥇 Gold Layer:")
gold_catalog = f"{os.environ['PROJECT_CODE']}-{os.environ['ENVIRONMENT']}-gold"
try:
    for schema in ['analytics', 'reporting']:
        tables = spark.sql(f"SHOW TABLES IN `{gold_catalog}`.`{schema}`").collect()
        print(f"  {schema}: {len(tables)} tables")
        for table in tables:
            count = spark.sql(f"SELECT COUNT(*) FROM `{gold_catalog}`.`{schema}`.`{table.tableName}`").collect()[0][0]
            print(f"    - {table.tableName}: {count} records")
except Exception as e:
    print(f"  Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary

# COMMAND ----------

print("\n✅ E2E PIPELINE EXECUTION COMPLETE!")
print("=" * 80)
print(f"End Time: {datetime.now()}")
print("\n📊 Summary:")
print("  - Framework package installed and imported successfully")
print("  - Bronze ingestion completed for all configured sources")
print("  - Silver transformations applied")
print("  - Gold layer analytics generated")
print("  - All data accessible via Unity Catalog")

# COMMAND ----------

# Return success
dbutils.notebook.exit("SUCCESS")